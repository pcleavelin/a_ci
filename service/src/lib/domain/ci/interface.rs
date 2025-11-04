use super::models::repo::Repository;
use crate::outbound::resource::Resource;

pub trait CiService: Send + Sync + Clone + 'static {
    fn repos(&self) -> Vec<Repository>;
}
