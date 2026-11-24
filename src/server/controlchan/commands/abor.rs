//! The RFC 959 Abort (`ABOR`) command
//
// This command tells the server to abort the previous FTP
// service command and any associated transfer of data. The
// abort command may require "special action", as discussed in
// the Section on FTP Commands, to force recognition by the
// server.  No action is to be taken if the previous command
// has been completed (including data transfer).  The control
// connection is not to be closed by the server, but the data
// connection must be closed.

use crate::server::chancomms::ControlChanMsg;
use crate::server::controlchan::{
    Reply, ReplyCode,
    error::ControlChanError,
    handler::{CommandContext, CommandHandler},
};
use async_trait::async_trait;
use tokio::sync::mpsc::error::TrySendError;
use unftp_core::auth::UserDetail;
use unftp_core::storage::{Metadata, StorageBackend};

#[derive(Debug)]
pub struct Abor;

#[async_trait]
impl<Storage, User> CommandHandler<Storage, User> for Abor
where
    Storage: StorageBackend<User> + 'static,
    Storage::Metadata: Metadata,
    User: UserDetail + 'static,
{
    #[tracing_attributes::instrument]
    async fn handle(&self, args: CommandContext<Storage, User>) -> Result<Reply, ControlChanError> {
        let session = args.session.lock().await;
        let Some(tx) = session.data_abort_tx.as_ref() else {
            return Ok(Reply::new(ReplyCode::ClosingDataConnection, "Data channel already closed"));
        };
        if session.data_abort_rx.is_some() {
            // The client has not opened the data connection, so no transfer is in progress.
            return Ok(Reply::new(ReplyCode::ClosingDataConnection, "Closed data channel"));
        }
        // ABOR is answered after the transfer it aborts (RFC 959, section 4.1.3), so the data channel answers
        // it once it has answered its command. Once the data channel is done, it closes the channel, and ABOR
        // is answered here.
        let sent = tx.try_send(());
        match sent {
            Ok(()) => Ok(Reply::none()),
            Err(TrySendError::Full(())) => {
                // An earlier ABOR is still waiting for the data channel. This one waits behind it.
                let tx = tx.clone();
                let control_msg_tx = args.tx_control_chan;
                let logger = args.logger;
                tokio::spawn(async move {
                    let sent = tx.send(()).await;
                    if sent.is_err() {
                        let reply = Reply::new(ReplyCode::ClosingDataConnection, "Data channel already closed");
                        let replied = control_msg_tx.send(ControlChanMsg::CommandChannelReply(reply)).await;
                        if let Err(err) = replied {
                            slog::warn!(logger, "Could not send the ABOR reply to the control channel: {}", err);
                        }
                    }
                });
                Ok(Reply::none())
            }
            Err(TrySendError::Closed(())) => Ok(Reply::new(ReplyCode::ClosingDataConnection, "Data channel already closed")),
        }
    }
}
