"""Simple Consensus Mechanism for Constellation.

This implements a lightweight consensus protocol for critical decisions
without the complexity of full Raft. Uses quorum-based voting.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

from constellation.network.peer_communication import (
    MessageType,
)

if TYPE_CHECKING:
    from typing import Callable

    from constellation.network.peer_communication import (
        NodeState,
        PeerCommunicator,
        PeerMessage,
    )


logger = logging.getLogger(__name__)


class ProposalType(Enum):
    """Types of proposals that require consensus."""

    CONTAINER_PLACEMENT = "container_placement"
    NODE_EVICTION = "node_eviction"
    CLUSTER_CONFIG_CHANGE = "cluster_config_change"
    FAILOVER_DECISION = "failover_decision"


@dataclass
class Proposal:
    """A proposal that requires consensus."""

    proposal_id: str
    proposal_type: ProposalType
    proposer_id: str
    data: dict[str, Any]
    timestamp: float
    timeout: float = 30.0  # 30 seconds to reach consensus

    def __post_init__(self):
        if not self.proposal_id:
            self.proposal_id = str(uuid.uuid4())


@dataclass
class Vote:
    """A vote on a proposal."""

    proposal_id: str
    voter_id: str
    vote: bool  # True = yes, False = no
    timestamp: float


class ConsensusManager:
    """Manages consensus for critical decisions."""

    def __init__(self, node_id: str, peer_communicator: PeerCommunicator):
        self.node_id: str = node_id
        self.peer_communicator: PeerCommunicator = peer_communicator

        # Active proposals
        self.active_proposals: dict[str, Proposal] = {}
        self.proposal_votes: dict[str, list[Vote]] = {}
        self.my_votes: dict[str, bool] = {}

        # Consensus callbacks
        self.consensus_callbacks: dict[ProposalType, Callable] = {}

        # Register message handlers
        from constellation.network.peer_communication import MessageType

        peer_communicator.register_handler(
            MessageType.CONSENSUS_PROPOSAL,
            self._handle_proposal,
        )
        peer_communicator.register_handler(
            MessageType.CONSENSUS_VOTE,
            self._handle_vote,
        )

        # Start cleanup task
        asyncio.create_task(self._cleanup_expired_proposals())

    def register_consensus_callback(
        self,
        proposal_type: ProposalType,
        callback: Callable,
    ):
        """Register callback for when consensus is reached."""
        self.consensus_callbacks[proposal_type] = callback

    async def propose(
        self,
        proposal_type: ProposalType,
        data: dict[str, Any],
    ) -> bool:
        """Propose something that requires consensus."""
        proposal = Proposal(
            proposal_id=str(uuid.uuid4()),
            proposal_type=proposal_type,
            proposer_id=self.node_id,
            data=data,
            timestamp=time.time(),
        )

        logger.info(f"Proposing {proposal_type.value}: {proposal.proposal_id}")

        # Store proposal
        self.active_proposals[proposal.proposal_id] = proposal
        self.proposal_votes[proposal.proposal_id] = []

        # Vote yes on our own proposal
        await self._cast_vote(proposal.proposal_id, True)

        # Broadcast proposal to all peers
        await self.peer_communicator._broadcast_message(
            MessageType.CONSENSUS_PROPOSAL,
            {
                "proposal_id": proposal.proposal_id,
                "proposal_type": proposal_type.value,
                "proposer_id": proposal.proposer_id,
                "data": proposal.data,
                "timestamp": proposal.timestamp,
                "timeout": proposal.timeout,
            },
        )

        # Wait for consensus or timeout
        start_time: float = time.time()
        while time.time() - start_time < proposal.timeout:
            if await self._check_consensus(proposal.proposal_id):
                logger.info(f"Consensus reached for proposal {proposal.proposal_id}")
                return True
            await asyncio.sleep(0.5)

        logger.warning(f"Consensus timeout for proposal {proposal.proposal_id}")
        self._cleanup_proposal(proposal.proposal_id)
        return False

    async def _handle_proposal(self, message: PeerMessage):
        """Handle incoming consensus proposal."""
        data: dict[str, Any] = message.data
        proposal_id: str = data["proposal_id"]

        # Don't handle our own proposals
        if data["proposer_id"] and data["proposer_id"].strip() == (
            self.node_id or self.node_id.strip()
        ):
            return

        # Create proposal object
        proposal = Proposal(
            proposal_id=proposal_id,
            proposal_type=ProposalType(data["proposal_type"]),
            proposer_id=data["proposer_id"] or "",
            data=data["data"],
            timestamp=data["timestamp"],
            timeout=data.get("timeout", 30.0),
        )

        # Check if proposal is still valid (not expired)
        if time.time() - proposal.timestamp > proposal.timeout:
            logger.debug(f"Ignoring expired proposal {proposal_id}")
            return

        # Store proposal
        self.active_proposals[proposal_id] = proposal
        if proposal_id not in self.proposal_votes:
            self.proposal_votes[proposal_id] = []

        # Decide how to vote
        vote_decision: bool = await self._evaluate_proposal(proposal)
        await self._cast_vote(proposal_id, vote_decision)

    async def _handle_vote(self, message: PeerMessage):
        """Handle incoming vote."""
        data: dict[str, Any] = message.data
        proposal_id: str = data["proposal_id"]

        if proposal_id not in self.active_proposals:
            logger.debug(f"Received vote for unknown proposal {proposal_id}")
            return

        vote: Vote = Vote(
            proposal_id=proposal_id,
            voter_id=data["voter_id"] or "",
            vote=data["vote"],
            timestamp=data["timestamp"],
        )

        # Add vote if not already present
        existing_votes: list[Vote] = self.proposal_votes.get(proposal_id, [])
        if not any(v.voter_id == vote.voter_id for v in existing_votes):
            existing_votes.append(vote)
            self.proposal_votes[proposal_id] = existing_votes

            logger.debug(f"Received vote from {vote.voter_id} for {proposal_id}: {vote.vote}")

            # Check if consensus reached
            if await self._check_consensus(proposal_id):
                await self._execute_consensus(proposal_id)

    async def _cast_vote(self, proposal_id: str, vote: bool):
        """Cast our vote on a proposal."""
        self.my_votes[proposal_id] = vote

        # Add our vote to the local list
        if proposal_id in self.proposal_votes:
            # Remove any existing vote from us
            self.proposal_votes[proposal_id] = [
                v
                for v in self.proposal_votes[proposal_id]
                if v.voter_id != self.node_id
            ]

            # Add our new vote
            self.proposal_votes[proposal_id].append(
                Vote(
                    proposal_id=proposal_id,
                    voter_id=self.node_id,
                    vote=vote,
                    timestamp=time.time(),
                )
            )

        # Broadcast vote
        await self.peer_communicator._broadcast_message(
            MessageType.CONSENSUS_VOTE,
            {
                "proposal_id": proposal_id,
                "voter_id": self.node_id,
                "vote": vote,
                "timestamp": time.time(),
            },
        )

        logger.debug(f"Cast vote {vote} for proposal {proposal_id}")

    async def _evaluate_proposal(
        self,
        proposal: Proposal,
    ) -> bool:
        """Evaluate whether to vote yes or no on a proposal."""
        # This is where you implement your voting logic
        # For now, simple heuristics:

        if proposal.proposal_type == ProposalType.CONTAINER_PLACEMENT:
            # Vote yes if we have capacity and the container isn't already running
            container_id: str | None = proposal.data.get("container_id")
            target_node: str | None = proposal.data.get("target_node")

            # Don't place on unhealthy nodes
            if target_node in self.peer_communicator.peers:
                peer: NodeState = self.peer_communicator.peers[target_node]
                if not peer.is_healthy:
                    return False

            # Check if container already exists somewhere
            for peer in self.peer_communicator.peers.values():
                if container_id in peer.containers:
                    return False  # Already exists

            return True

        elif proposal.proposal_type == ProposalType.NODE_EVICTION:
            # Vote yes if node is actually unhealthy
            target_node: str | None = proposal.data.get("node_id")
            if target_node in self.peer_communicator.peers:
                peer = self.peer_communicator.peers[target_node]
                # Vote yes if node hasn't been seen for a while
                return time.time() - peer.last_seen > 60
            return True  # Unknown node, ok to evict

        elif proposal.proposal_type == ProposalType.FAILOVER_DECISION:
            # Vote yes if the source node is actually down
            source_node: str | None = proposal.data.get("source_node")
            if source_node in self.peer_communicator.peers:
                peer = self.peer_communicator.peers[source_node]
                return not peer.is_healthy
            return True  # Unknown node, ok to failover

        # Default: vote yes for config changes
        return True

    async def _check_consensus(self, proposal_id: str) -> bool:
        """Check if consensus has been reached for a proposal."""
        if proposal_id not in self.proposal_votes:
            return False

        votes: list[Vote] = self.proposal_votes[proposal_id]
        healthy_peers: list[NodeState] = self.peer_communicator.get_healthy_peers()
        total_nodes: int = len(healthy_peers) + 1  # +1 for ourselves

        # Need majority (more than half)
        required_votes: int = (total_nodes // 2) + 1

        yes_votes: int = sum(1 for vote in votes if vote.vote)
        no_votes: int = sum(1 for vote in votes if not vote.vote)

        # Consensus reached if we have majority yes votes
        if yes_votes >= required_votes:
            return True

        # Or if majority voted no (consensus to reject)
        if no_votes >= required_votes:
            return True

        return False

    async def _execute_consensus(self, proposal_id: str):
        """Execute the consensus decision."""
        if proposal_id not in self.active_proposals:
            return

        proposal: Proposal = self.active_proposals[proposal_id]
        votes: list[Vote] = self.proposal_votes[proposal_id]

        yes_votes: int = sum(1 for vote in votes if vote.vote)
        no_votes: int = sum(1 for vote in votes if not vote.vote)

        consensus_result: bool = yes_votes > no_votes

        logger.info(f"Consensus for {proposal_id}: {'APPROVED' if consensus_result else 'REJECTED'}")

        # Execute callback if approved
        if consensus_result and proposal.proposal_type in self.consensus_callbacks:
            try:
                await self.consensus_callbacks[proposal.proposal_type](proposal.data)
            except Exception:
                logger.exception("Error executing consensus callback")

        # Cleanup
        self._cleanup_proposal(proposal_id)

    def _cleanup_proposal(self, proposal_id: str):
        """Clean up a finished proposal."""
        self.active_proposals.pop(proposal_id, None)
        self.proposal_votes.pop(proposal_id, None)
        self.my_votes.pop(proposal_id, None)

    async def _cleanup_expired_proposals(self):
        """Periodically clean up expired proposals."""
        while True:
            try:
                current_time: float = time.time()
                expired_proposals: list[str] = []

                for proposal_id, proposal in self.active_proposals.items():
                    if current_time - proposal.timestamp > proposal.timeout:
                        expired_proposals.append(proposal_id)

                for proposal_id in expired_proposals:
                    logger.debug(f"Cleaning up expired proposal {proposal_id}")
                    self._cleanup_proposal(proposal_id)

                await asyncio.sleep(10)  # Check every 10 seconds
            except Exception:
                logger.exception("Error in proposal cleanup")
                await asyncio.sleep(10)
