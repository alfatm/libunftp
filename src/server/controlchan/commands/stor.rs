//! The RFC 959 Store (`STOR`) command
//
// This command causes the server-DTP to accept the data
// transferred via the data connection and to store the data as
// a file at the server site.  If the file specified in the
// pathname exists at the server site, then its contents shall
// be replaced by the data being transferred.  A new file is
// created at the server site if the file specified in the
// pathname does not already exist.

use crate::server::chancomms::DataChanCmd;
use crate::{
    auth::UserDetail,
    server::controlchan::{
        Reply, ReplyCode,
        command::Command,
        error::ControlChanError,
        handler::{CommandContext, CommandHandler},
    },
    storage::{Metadata, StorageBackend},
};
use async_trait::async_trait;

#[derive(Debug)]
pub struct Stor;

#[async_trait]
impl<Storage, User> CommandHandler<Storage, User> for Stor
where
    User: UserDetail + 'static,
    Storage: StorageBackend<User> + 'static,
    Storage::Metadata: Metadata,
{
    #[tracing_attributes::instrument]
    async fn handle(&self, args: CommandContext<Storage, User>) -> Result<Reply, ControlChanError> {
        let mut session = args.session.lock().await;
        // A REST offset applies to the transfer command right after it (RFC 959, section 4.1.3), so this
        // command consumes it, whether or not the transfer then starts.
        let start_pos = std::mem::take(&mut session.start_pos);

        let (cmd, path): (DataChanCmd, String) = match args.parsed_command.clone() {
            Command::Stor { path } => {
                let path_clone = path.clone();
                (DataChanCmd::Stor { path, start_pos }, path_clone)
            }
            _ => panic!("Programmer error, expected command to be STOR"),
        };

        let logger = args.logger;
        match session.data_cmd_tx.take() {
            Some(tx) => {
                // Queued before the next command is read, so that the data channel gets it before an ABOR
                // that follows it.
                let sent = tx.try_send(cmd);
                if let Err(err) = sent {
                    slog::warn!(logger, "STOR: could not notify data channel to respond with STOR. {}", err);
                    return Ok(Reply::new(ReplyCode::CantOpenDataConnection, "No data connection established"));
                }
                Ok(Reply::new(ReplyCode::FileStatusOkay, "Ready to receive data"))
            }
            None => {
                slog::warn!(logger, "STOR: no data connection established for STORing {:?}", path);

                Ok(Reply::new(ReplyCode::CantOpenDataConnection, "No data connection established"))
            }
        }
    }
}
