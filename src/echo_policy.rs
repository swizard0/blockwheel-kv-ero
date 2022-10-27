use crate::{
    proto,
    ftd_sklave,
};

use arbeitssklave::{
    komm,
};

pub struct EchoPolicy;

impl blockwheel_kv::EchoPolicy for EchoPolicy {
    type Info = komm::Rueckkopplung<ftd_sklave::Order, proto::RequestInfoReplyTx>;
    type Insert = komm::Rueckkopplung<ftd_sklave::Order, proto::RequestInsertReplyTx>;
    type LookupRange = komm::Rueckkopplung<ftd_sklave::Order, ftd_sklave::LookupKind>;
    type Remove = komm::Rueckkopplung<ftd_sklave::Order, proto::RequestRemoveReplyTx>;
    type Flush = komm::Rueckkopplung<ftd_sklave::Order, proto::RequestFlushReplyTx>;
}
