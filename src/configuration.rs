use nota_codec::NotaRecord;
use signal_orchestrate::WirePath;

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct DaemonConfiguration {
    pub store_path: WirePath,
    pub ordinary_socket_path: WirePath,
    pub meta_socket_path: WirePath,
    pub upgrade_socket_path: WirePath,
    pub workspace_root: WirePath,
    pub git_index_root: WirePath,
}

impl DaemonConfiguration {
    pub fn new(
        store_path: WirePath,
        ordinary_socket_path: WirePath,
        meta_socket_path: WirePath,
        upgrade_socket_path: WirePath,
        workspace_root: WirePath,
        git_index_root: WirePath,
    ) -> Self {
        Self {
            store_path,
            ordinary_socket_path,
            meta_socket_path,
            upgrade_socket_path,
            workspace_root,
            git_index_root,
        }
    }
}
