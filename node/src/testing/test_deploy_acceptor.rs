use serde::Serialize;
use std::fmt;
use std::fmt::{Display, Formatter};
use tracing::debug;

use crate::{
    components::{deploy_acceptor::Error, Component},
    effect::{
        announcements::DeployAcceptorAnnouncement,
        requests::{ContractRuntimeRequest, StorageRequest},
        EffectBuilder, EffectExt, Effects, Responder,
    },
    types::{chainspec::DeployConfig, Chainspec, Deploy, Timestamp},
    utils::Source,
    NodeRng,
};
use casper_types::ProtocolVersion;

/// `DeployAcceptor` events.
#[derive(Debug, Serialize)]
pub(crate) enum Event {
    /// The initiating event to accept a new `Deploy`.
    Accept {
        deploy: Box<Deploy>,
        source: Source,
        maybe_responder: Option<Responder<Result<(), Error>>>,
    },
    /// The result of the `DeployAcceptor` putting a `Deploy` to the storage component.
    PutToStorageResult {
        event_metadata: EventMetadata,
        is_new: bool,
    },
}

impl Display for Event {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Event::Accept { deploy, source, .. } => {
                write!(formatter, "accept {} from {}", deploy.id(), source)
            }
            Event::PutToStorageResult {
                event_metadata,
                is_new,
                ..
            } => {
                if *is_new {
                    write!(
                        formatter,
                        "put new {} to storage",
                        event_metadata.deploy.id()
                    )
                } else {
                    write!(
                        formatter,
                        "had already stored {}",
                        event_metadata.deploy.id()
                    )
                }
            }
        }
    }
}

/// A utility struct to hold duplicated information across events.
#[derive(Debug, Serialize)]
pub(crate) struct EventMetadata {
    pub(super) deploy: Box<Deploy>,
    pub(super) source: Source,
    pub(super) maybe_responder: Option<Responder<Result<(), Error>>>,
}

impl EventMetadata {
    pub(crate) fn new(
        deploy: Box<Deploy>,
        source: Source,
        maybe_responder: Option<Responder<Result<(), Error>>>,
    ) -> Self {
        EventMetadata {
            deploy,
            source,
            maybe_responder,
        }
    }
}

/// A helper trait constraining `DeployAcceptor` compatible reactor events.
pub(crate) trait ReactorEventT:
    From<Event>
    + From<DeployAcceptorAnnouncement>
    + From<StorageRequest>
    + From<ContractRuntimeRequest>
    + Send
{
}

impl<REv> ReactorEventT for REv where
    REv: From<Event>
        + From<DeployAcceptorAnnouncement>
        + From<StorageRequest>
        + From<ContractRuntimeRequest>
        + Send
{
}

#[derive(Debug)]
pub struct DeployAcceptor {}

impl DeployAcceptor {
    pub(crate) fn new() -> Result<Self, prometheus::Error> {
        Ok(DeployAcceptor {})
    }

    fn accept<REv: ReactorEventT>(
        &mut self,
        effect_builder: EffectBuilder<REv>,
        deploy: Box<Deploy>,
        source: Source,
        maybe_responder: Option<Responder<Result<(), Error>>>,
    ) -> Effects<Event> {
        let event_metadata = EventMetadata::new(deploy.clone(), source, maybe_responder);
        effect_builder
            .put_deploy_to_storage(Box::new(*deploy))
            .event(move |is_new| Event::PutToStorageResult {
                event_metadata,
                is_new,
            })
    }

    fn handle_put_to_storage<REv: ReactorEventT>(
        &self,
        effect_builder: EffectBuilder<REv>,
        event_metadata: EventMetadata,
        is_new: bool,
    ) -> Effects<Event> {
        let EventMetadata {
            deploy,
            source,
            maybe_responder,
        } = event_metadata;
        let mut effects = Effects::new();
        if is_new {
            effects.extend(
                effect_builder
                    .announce_new_deploy_accepted(deploy, source)
                    .ignore(),
            );
        }

        // success
        if let Some(responder) = maybe_responder {
            effects.extend(responder.respond(Ok(())).ignore());
        }
        effects
    }
}

impl<REv: ReactorEventT> Component<REv> for DeployAcceptor {
    type Event = Event;
    type ConstructionError = ();

    fn handle_event(
        &mut self,
        effect_builder: EffectBuilder<REv>,
        rng: &mut NodeRng,
        event: Self::Event,
    ) -> Effects<Self::Event> {
        debug!(?event, "handling event");
        match event {
            Event::Accept {
                deploy,
                source,
                maybe_responder,
            } => self.accept(effect_builder, deploy, source, maybe_responder),
            Event::PutToStorageResult {
                event_metadata,
                is_new,
            } => self.handle_put_to_storage(effect_builder, event_metadata, is_new),
        }
    }
}
