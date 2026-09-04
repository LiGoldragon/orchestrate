#![allow(dead_code)]
pub struct Situated(pub Option<protos::Extent>, pub datomic::Fault);
impl datomic::Corporal<datomic::Datom> for Situated {
    type Fault = datomic::Fault;
    fn incorporate(concept: datomic::Datom) -> std::result::Result<Self, datomic::Fault> {
        match concept {
            datomic::Datom::Struct(fields) if fields.len() == 2usize => {
                let mut iter = fields.into_iter();
                Ok(Self(
                    <Option<protos::Extent> as datomic::Corporal<datomic::Datom>>::incorporate(
                        iter.next().unwrap(),
                    )?,
                    <datomic::Fault as datomic::Corporal<datomic::Datom>>::incorporate(
                        iter.next().unwrap(),
                    )?,
                ))
            }
            datomic::Datom::Struct(fields) => Err(datomic::Fault::Corporal(
                vec![],
                datomic::Problem::Arity(2i64, fields.len() as i64),
            )),
            other => Err(datomic::Fault::Corporal(
                vec![],
                datomic::Problem::Shape(datomic::Expected::Struct, other),
            )),
        }
    }
}
impl datomic::Datomic for Situated {
    fn datomize(&self) -> datomic::Datom {
        datomic::Datom::Struct(vec![
            datomic::Datomic::datomize(&self.0),
            datomic::Datomic::datomize(&self.1),
        ])
    }
}
pub struct ClientFailureUnreachable(pub protos::Text, pub protos::Text);
pub enum ClientFailure {
    Unreadable(Situated),
    Unreachable(ClientFailureUnreachable),
    Refused(signal_orchestrate::Refusal),
}
impl datomic::Corporal<datomic::Datom> for ClientFailure {
    type Fault = datomic::Fault;
    fn incorporate(concept: datomic::Datom) -> std::result::Result<Self, datomic::Fault> {
        match concept {
            datomic::Datom::Variant(head, protos::Separator::Period, Some(body))
                if head == stringify!(Unreadable) =>
            {
                Ok(Self::Unreadable(<Situated as datomic::Corporal<
                    datomic::Datom,
                >>::incorporate(*body)?))
            }
            datomic::Datom::Variant(head, protos::Separator::Period, Some(body))
                if head == stringify!(Unreachable) =>
            {
                Ok(Self::Unreachable(
                    <ClientFailureUnreachable as datomic::Corporal<datomic::Datom>>::incorporate(
                        *body,
                    )?,
                ))
            }
            datomic::Datom::Variant(head, protos::Separator::Period, Some(body))
                if head == stringify!(Refused) =>
            {
                Ok(
                    Self::Refused(<signal_orchestrate::Refusal as datomic::Corporal<
                        datomic::Datom,
                    >>::incorporate(*body)?),
                )
            }
            other => Err(datomic::Fault::Corporal(
                vec![],
                datomic::Problem::Shape(datomic::Expected::Variant, other),
            )),
        }
    }
}
impl datomic::Datomic for ClientFailure {
    fn datomize(&self) -> datomic::Datom {
        match self {
            Self::Unreadable(value) => datomic::Datom::Variant(
                stringify!(Unreadable).to_owned(),
                protos::Separator::Period,
                Some(Box::new(datomic::Datomic::datomize(value))),
            ),
            Self::Unreachable(value) => datomic::Datom::Variant(
                stringify!(Unreachable).to_owned(),
                protos::Separator::Period,
                Some(Box::new(datomic::Datomic::datomize(value))),
            ),
            Self::Refused(value) => datomic::Datom::Variant(
                stringify!(Refused).to_owned(),
                protos::Separator::Period,
                Some(Box::new(datomic::Datomic::datomize(value))),
            ),
        }
    }
}
impl datomic::Corporal<datomic::Datom> for ClientFailureUnreachable {
    type Fault = datomic::Fault;
    fn incorporate(concept: datomic::Datom) -> std::result::Result<Self, datomic::Fault> {
        match concept {
            datomic::Datom::Struct(fields) if fields.len() == 2usize => {
                let mut iter = fields.into_iter();
                Ok(Self(
                    <protos::Text as datomic::Corporal<datomic::Datom>>::incorporate(
                        iter.next().unwrap(),
                    )?,
                    <protos::Text as datomic::Corporal<datomic::Datom>>::incorporate(
                        iter.next().unwrap(),
                    )?,
                ))
            }
            datomic::Datom::Struct(fields) => Err(datomic::Fault::Corporal(
                vec![],
                datomic::Problem::Arity(2i64, fields.len() as i64),
            )),
            other => Err(datomic::Fault::Corporal(
                vec![],
                datomic::Problem::Shape(datomic::Expected::Struct, other),
            )),
        }
    }
}
impl datomic::Datomic for ClientFailureUnreachable {
    fn datomize(&self) -> datomic::Datom {
        datomic::Datom::Struct(vec![
            datomic::Datomic::datomize(&self.0),
            datomic::Datomic::datomize(&self.1),
        ])
    }
}
