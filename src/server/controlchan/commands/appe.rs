//! The RFC 959 Append (`APPE`) command
//
// This command causes the server-DTP to accept the data
// transferred via the data connection and to store the data in
// a file at the server site.  If the file specified in the
// pathname exists at the server site, the data shall be
// appended to that file; otherwise the file shall be created.

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
pub struct Appe;

#[async_trait]
impl<Storage, User> CommandHandler<Storage, User> for Appe
where
    User: UserDetail + 'static,
    Storage: StorageBackend<User> + 'static,
    Storage::Metadata: Metadata,
{
    #[tracing_attributes::instrument]
    async fn handle(&self, args: CommandContext<Storage, User>) -> Result<Reply, ControlChanError> {
        let mut session = args.session.lock().await;
        // APPE writes at the end of the file, so a REST offset right before it has no effect. It is
        // consumed all the same: it applies to the transfer command right after it (RFC 959, section 4.1.3).
        session.start_pos = 0;

        let (cmd, path): (DataChanCmd, String) = match args.parsed_command.clone() {
            Command::Appe { path } => {
                let path_clone = path.clone();
                (DataChanCmd::Appe { path }, path_clone)
            }
            _ => panic!("Programmer error, expected command to be APPE"),
        };

        let logger = args.logger;
        match session.data_cmd_tx.take() {
            Some(tx) => {
                // Queued before the next command is read, so that the data channel gets it before an ABOR
                // that follows it.
                let sent = tx.try_send(cmd);
                if let Err(err) = sent {
                    slog::warn!(logger, "APPE: could not notify data channel. {}", err);
                    return Ok(Reply::new(ReplyCode::CantOpenDataConnection, "No data connection established"));
                }
                Ok(Reply::new(ReplyCode::FileStatusOkay, "Ready to receive data"))
            }
            None => {
                slog::warn!(logger, "APPE: no data connection established for APPEing {:?}", path);
                Ok(Reply::new(ReplyCode::CantOpenDataConnection, "No data connection established"))
            }
        }
    }
}
