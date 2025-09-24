pub mod sp1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobKind {
    // param: zkvm kind, batch id
    SP1(SP1Op, u128)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SP1Op {
    ProveSubblock,

    ProveAgg,
}
