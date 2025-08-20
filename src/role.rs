#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
    Candidate,
}

impl Role {
    pub fn transition_to_follower(&mut self) {
        *self = Role::Follower;
    }
    pub fn transition_to_leader(&mut self) {
        *self = Role::Leader;
    }
    pub fn transition_to_candidate(&mut self) {
        *self = Role::Candidate;
    }
    pub fn is_leader(&self) -> bool {
        matches!(self, Role::Leader)
    }
    pub fn is_follower(&self) -> bool {
        matches!(self, Role::Follower)
    }
    pub fn is_candidate(&self) -> bool {
        matches!(self, Role::Candidate)
    }
}
