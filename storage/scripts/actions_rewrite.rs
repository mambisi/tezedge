use clap::{App, Arg};

mod actions_tool;

use actions_tool::{ActionsFileReader, ContextAction, ActionsFileWriter, ActionsFileHeader};

struct Args {
    input: String,
    output: String,
    limit: usize,
}

impl Args {
    pub fn read_args() -> Self {
        let app = App::new("action file Rewrite")
            .about("rewrites actions file")
            .arg(Arg::with_name("input")
                .required(true))
            .arg(Arg::with_name("output")
                .required(true)
            )
            .arg(Arg::with_name("limit")
                .long("limit")
                .short("l")
                .default_value("362291")
            );

        let matches = app.get_matches();


        Self {
            input: matches.value_of("input").unwrap().to_string(),
            output: matches.value_of("input").unwrap().to_string(),
            limit: matches.value_of("limit").unwrap().parse::<usize>().unwrap(),
        }
    }
}

fn main() {
    rewrite_action_file(Args::read_args())
}

fn rewrite_action_file(args: Args) {
    let limit = args.limit;
    let mut writer = ActionsFileWriter::new(args.output).unwrap();
    let reader = ActionsFileReader::new(args.input).unwrap();
    reader.for_each(|(block, actions)| {

        let k = block.block_level;

        if (k as usize) < limit {
            println!("BLOCK :{}",k );
            writer.update(block, actions);
        }
    });
}