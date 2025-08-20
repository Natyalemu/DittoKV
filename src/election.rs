// Role cycle: a simple sequence showing the cycle each node goes through .
// 1) Election is held according to majority vote
// 2) Each candidate request vote from all other node
// 3) A candidate that receives votes from a majority of the nodes transition to leader
// 4) If the leader recieves rpc with a term higher than its current term it transitions to follower.
// 5) If a follower times without hearing from leader it becomes a candidate and start election
//    otherwise it remains in follower state.
// 6) This Cycle repeates
