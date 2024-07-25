use clap::{Args, Subcommand};
use csv::Writer;
use prettytable::Table;
use serde::Serialize;

use feastore::{database::metadata::ListOpt, Store};

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Command {
    /// show detailed information
    #[arg(short, long, global(true))]
    wide: bool,

    /// names
    names: Vec<String>,

    /// output format
    #[arg(value_enum, default_value_t=Format::AsciiTable, short, long, global(true))]
    format: Format,

    #[command(subcommand)]
    cmds: SubCmd,
}

#[derive(Debug, Subcommand)]
enum SubCmd {
    /// Get existing entities given specific conditions
    Entity,
    /// Get existing groups given specific conditions
    Group,
    /// Get existing features given specific conditions
    Feature,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum Format {
    Csv,
    Yaml,
    AsciiTable,
}

impl Command {
    pub async fn run(&self, store: Store) {
        let opt: ListOpt = if self.names.is_empty() {
            ListOpt::All
        } else {
            ListOpt::from(&self.names)
        };

        match &self.cmds {
            SubCmd::Entity => {
                let entities = store
                    .list_entity(opt)
                    .await
                    .expect("failed to get entities");

                output(entities, &self.format)
            }
            SubCmd::Group => {
                let groups = store.list_group(opt).await.expect("failed to get groups");

                output(groups, &self.format)
            }
            SubCmd::Feature => {
                let features = store
                    .list_feature(opt)
                    .await
                    .expect("failed to get features");

                output(features, &self.format)
            }
        }
    }
}

fn output<T: Serialize>(values: Vec<T>, format: &Format) {
    match format {
        Format::Yaml => {
            let yaml = serde_yaml::to_string(&values).unwrap();
            println!("{}", yaml);
        }
        Format::Csv => {
            let data = to_csv_string(values);
            println!("{}", data);
        }
        Format::AsciiTable => {
            let data = to_csv_string(values);
            let table = Table::from_csv_string(&data).unwrap();
            table.printstd();
        }
    }
}

fn to_csv_string<S: Serialize>(serize: Vec<S>) -> String {
    let mut wtr = Writer::from_writer(vec![]);
    for s in serize {
        wtr.serialize(s).unwrap();
    }
    String::from_utf8(wtr.into_inner().unwrap()).unwrap()
}
