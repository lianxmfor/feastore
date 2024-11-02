#[derive(Debug)]
pub enum GetOpt<'a> {
    ID(i64),
    Name(&'a str),
}

#[derive(Debug)]
pub enum ListOpt<'a> {
    All,
    IDs(Vec<i64>),
    Names(Vec<&'a str>),
}

impl<'a> From<&'a Vec<String>> for ListOpt<'a> {
    fn from(names: &'a Vec<String>) -> Self {
        ListOpt::Names(Vec::from_iter(names.iter().map(String::as_str)))
    }
}

impl<'a> From<Vec<&'a str>> for ListOpt<'a> {
    fn from(names: Vec<&'a str>) -> Self {
        ListOpt::Names(names)
    }
}

#[derive(Debug)]
pub enum ListGroupOpt {
    EntityIDs(Vec<i64>),
    GroupIDs(Vec<i64>),
}

#[derive(Debug)]
pub enum ListFeatureOpt {
    EntityIDs(Vec<i64>),
    GroupIDs(Vec<i64>),
    FeatureIDs(Vec<i64>),
}
