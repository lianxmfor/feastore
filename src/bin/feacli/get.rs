use clap::{Args, Subcommand};
use csv::Writer;
use prettytable::Table;
use serde::Serialize;

use feastore::database::metadata::{ApplyEntity, ApplyFeature, ApplyGroup, ListOpt};
use feastore::{Result, Store};

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Command {
    /// show detailed information
    #[arg(short, long, global(true))]
    wide: bool,

    /// names
    #[arg(short, long, global(true))]
    names: Vec<String>,

    /// output format
    #[arg(value_enum, default_value_t=Format::AsciiTable, short, long, global(true))]
    output: Format,

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
    pub async fn run(&self, store: Store) -> Result<()> {
        match &self.cmds {
            SubCmd::Entity => self.get_entity(store).await,
            SubCmd::Group => self.get_group(store).await,
            SubCmd::Feature => self.get_feature(store).await,
        }
    }

    async fn get_entity(&self, store: Store) -> Result<()> {
        let opt = build_opt(&self.names);
        match self.output {
            Format::Yaml => {
                let entities = store
                    .list_entity_with_full_information(opt)
                    .await?
                    .into_iter()
                    .map(|e| ApplyEntity::from(e))
                    .collect();

                output(entities, &self.output);
            }
            _ => {
                let entities = store
                    .list_entity(opt)
                    .await
                    .expect("failed to get entities");

                output(entities, &self.output);
            }
        }
        Ok(())
    }

    async fn get_group(&self, store: Store) -> Result<()> {
        let opt = build_opt(&self.names);
        match self.output {
            Format::Yaml => {
                let groups = store
                    .list_group_with_full_information(opt)
                    .await?
                    .into_iter()
                    .map(|g| ApplyGroup::from(g, true))
                    .collect();

                output(groups, &self.output);
            }
            _ => {
                let groups = store.list_group(opt).await?;

                output(groups, &self.output);
            }
        }
        Ok(())
    }

    async fn get_feature(&self, store: Store) -> Result<()> {
        let opt = build_opt(&self.names);
        match self.output {
            Format::Yaml => {
                let features = store
                    .list_feature(opt)
                    .await?
                    .into_iter()
                    .map(|f| ApplyFeature::from(f, true))
                    .collect();

                output(features, &self.output);
            }
            _ => {
                let entities = store
                    .list_feature(opt)
                    .await
                    .expect("failed to get entities");

                output(entities, &self.output);
            }
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct Items<T: Serialize> {
    items: Vec<T>,
}

fn output<T: Serialize>(values: Vec<T>, format: &Format) {
    if values.is_empty() {
        return;
    }

    match format {
        Format::Yaml => {
            let yaml = if values.len() == 1 {
                serde_yaml::to_string(values.first().unwrap()).unwrap()
            } else {
                let items = Items { items: values };
                serde_yaml::to_string(&items).unwrap()
            };
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

fn build_opt(names: &Vec<String>) -> ListOpt {
    if names.is_empty() {
        ListOpt::All
    } else {
        ListOpt::from(names)
    }
}
