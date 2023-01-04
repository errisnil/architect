use anyhow::Result;
use clap::Parser;
use native_tls::{Certificate, TlsConnector};
use postgres::{Client, NoTls};
use postgres_native_tls::MakeTlsConnector;
use serde::Deserialize;

#[derive(Deserialize, Default)]
struct Config {
    app: String,
    host: String,
    #[serde(default)]
    port: u16,
    dbname: String,
    user: String,
    #[serde(default)]
    password: String,
    // #[serde(default)]
    // passfile: String,
    #[serde(default)]
    connect_timeout_seconds: u16,
    #[serde(default)]
    ssl: bool,
    #[serde(default)]
    sslrootcert: String,
}

impl Config {
    fn defaults(&mut self) -> Result<()> {
        let home_dir = match home::home_dir() {
            Some(v) => v,
            None => {
                return Err(anyhow::anyhow!("couldn't read users home directory"));
            }
        };

        // if self.passfile.is_empty() {
        //     let passfile = if let Ok(v) = std::env::var("PGPASSFILE") {
        //         v
        //     } else {
        //         "".to_owned()
        //     };
        //     if passfile.is_empty() {
        //         self.passfile = format!("{:?}/.pgpass", &home_dir);
        //     } else {
        //         self.passfile = passfile;
        //     }
        // }

        if self.password.is_empty() {
            if let Ok(v) = std::env::var("PGPASSWORD") {
                self.password = v;
            }
        }

        if self.sslrootcert.is_empty() {
            let sslrootcert = if let Ok(v) = std::env::var("PGSSLROOTCERT") {
                v
            } else {
                "".to_owned()
            };
            if sslrootcert.is_empty() {
                self.sslrootcert = format!("{:?}/.postgresql/root.crt", &home_dir);
            } else {
                self.sslrootcert = sslrootcert;
            }
        }

        if self.port == 0 {
            self.port = 5432;
        }
        Ok(())
    }

    fn assert(&self) -> Result<()> {
        if self.host.is_empty() {
            return Err(anyhow::anyhow!("host cannot be empty"));
        }
        if self.dbname.is_empty() {
            return Err(anyhow::anyhow!("dbname cannot be empty"));
        }
        Ok(())
    }

    fn connect(&mut self) -> Result<Client> {
        self.defaults()?;
        let mut params = Vec::<String>::new();
        params.push(format!("host={}", &self.host));
        params.push(format!("port={}", &self.port));
        params.push(format!("dbname={}", &self.dbname));
        params.push("application_name=rust_migrator".to_string());
        params.push(format!("connect_timeout={}", &self.connect_timeout_seconds));
        if !self.user.is_empty() {
            params.push(format!("user={}", &self.user));
        }
        if !self.password.is_empty() {
            params.push(format!("password={}", &self.password));
        }
        // if !self.passfile.is_empty() {
        //     params.push(format!("passfile={}", &self.passfile));
        // }

        if self.ssl {
            eprintln!("ssl with cert: {}", &self.sslrootcert);
            params.push("sslmode=require".to_string());
            let mut connector = TlsConnector::builder();
            let connector = if std::path::PathBuf::from(&self.sslrootcert).exists() {
                eprintln!("using provided root certificate");
                // params.push(format!("sslrootcert={}", &self.sslrootcert));
                let cert = std::fs::read(&self.sslrootcert)?;
                let cert = Certificate::from_pem(&cert)?;
                connector.add_root_certificate(cert).build()?
            } else {
                eprintln!("using system certificate");
                connector.build()?
            };

            let connector = MakeTlsConnector::new(connector);
            eprintln!("Connection String: {}", &params.join(" "));
            return Ok(postgres::Client::connect(&params.join(" "), connector)?);
        }
        Ok(postgres::Client::connect(&params.join(" "), NoTls)?)
    }

    fn init(&mut self) -> Result<(Client, i64)> {
        let _ = self.assert()?;
        let mut client = self.connect()?;
        client.execute(
            "
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version BIGINT PRIMARY KEY,
                dirty BOOLEAN DEAFULT FALSE
            )
        ",
            &[],
        )?;
        let mut last_version: i64 = 0;
        for row in client.query(
            "SELECT version, dirty FROM schema_migrations ORDER BY version DESC LIMIT 1",
            &[],
        )? {
            let version: i64 = row.get(0);
            let dirty: bool = row.get(1);
            if dirty {
                return Err(anyhow::anyhow!(
                    "last version is dirty. migration had failed previously"
                ));
            }
            last_version = version;
            break;
        }
        Ok((client, last_version))
    }

    fn dir(&self, parent: &std::path::PathBuf) -> Result<std::path::PathBuf> {
        let mig_path = parent.join(&self.app);
        if mig_path.exists() && !mig_path.is_dir() {
            return Err(anyhow::anyhow!(format!("invalid path: {:?}", &mig_path)));
        }
        if !mig_path.exists() {
            let _ = std::fs::create_dir_all(&mig_path)?;
        }
        Ok(mig_path)
    }
}

struct Migrator {
    config: Config,
    dir: std::path::PathBuf,
    last_version: i64,
    client: Option<Client>,
    versions_available: Vec<i64>,
    initialized: bool,
}

impl Migrator {
    fn new(config: Config, dir: std::path::PathBuf) -> Result<Self> {
        let mut m = Migrator {
            config,
            dir,
            last_version: 0,
            client: None,
            versions_available: Vec::<i64>::new(),
            initialized: false,
        };
        m.init()?;
        Ok(m)
    }

    fn available_versions(&mut self) -> Result<()> {
        if !self.initialized {
            return Err(anyhow::anyhow!("Migrator not initialized"));
        }

        let reg = regex::Regex::new(r"^[1-9][0-9]*_(up|down)\.sql$")?;
        for f in std::fs::read_dir(&self.dir)? {
            let f = f?;
            let f = format!("{:?}", f.file_name());
            let caps = if let Some(v) = reg.captures(&f) {
                v
            } else {
                continue;
            };
            let mut ver = "".to_owned();
            if let Some(v) = caps.get(1) {
                ver = String::from(v.as_str());
            } else {
                continue;
            }
            if let Some(v) = caps.get(2) {
                ver = format!("{}{}", &ver, v.as_str());
            } else {
                continue;
            }

            match ver.parse::<i64>() {
                Ok(v) => self.versions_available.push(v),
                Err(e) => {
                    eprintln!("{:?}", e);
                }
            }
        }

        Ok(())
    }

    fn init(&mut self) -> Result<()> {
        let (client, last_version) = self.config.init()?;
        self.client = Some(client);
        self.last_version = last_version;
        self.dir = self.config.dir(&self.dir)?;
        self.initialized = true;
        self.available_versions()?;
        Ok(())
    }

    fn new_migration(&mut self) -> Result<()> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis();
        let up = self.dir.join(format!("{}_up.sql", ts));
        let down = self.dir.join(format!("{}_down.sql", ts));
        if up.exists() {
            return Err(anyhow::anyhow!(format!("file {:?} exists", &up)));
        }
        let _ = std::fs::File::create(&up)?;
        if down.exists() {
            return Err(anyhow::anyhow!(format!("file {:?} exists", &down)));
        }
        let _ = std::fs::File::create(&down)?;
        self.available_versions()?;
        println!("Versions available:\n{:?}", self.versions_available);
        Ok(())
    }
}

#[derive(Debug, Parser)]
#[command(author,version,about,long_about=None)]
struct Args {
    #[arg(short, long, default_value = "./migrations")]
    migdir: String,
    #[arg(short, long)]
    config: String,
}

fn read_config_toml(p: &std::path::PathBuf) -> Result<Config> {
    let cs = std::fs::read_to_string(p)?;
    Ok(toml::from_str(&cs)?)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let cp = std::path::PathBuf::from(&args.config);
    if !cp.exists() {
        return Err(anyhow::anyhow!("config path does not exist"));
    }
    let config: Config = read_config_toml(&cp)?;
    let dir = std::path::PathBuf::from(&args.migdir);

    let m = Migrator::new(config, dir)?;
    println!("{:?}", &m.versions_available);

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Once;
    static INIT: Once = Once::new();

    fn init() {
        INIT.call_once(|| {
            // Start dummy pg or cockroach server if needed
        });
    }

    #[test]
    fn new_migration() {
        init();

        let cs = r##"
        app = "test"
        dbname = "testpgmig"
        host = "localhost"
        port = 26257
        user = "testadmin"
        password = "testpass"
        ssl = true
        sslrootcert = "/Users/errisnil/code/cockroach/certs/ca.crt"
        "##;
        let config: crate::Config = toml::from_str(&cs).unwrap();
        let mut m = crate::Migrator::new(config, std::path::PathBuf::from("./migrations")).unwrap();
        m.new_migration().unwrap();
        let files = std::fs::read_dir("./migrations/test").unwrap();
        let mut count = 0;
        let mut up_exists = false;
        let mut down_exists = false;
        for f in files {
            count += 1;
            let f = f.unwrap();
            let file_name = f.file_name().to_str().unwrap().to_owned();
            if file_name.ends_with("_up.sql") {
                up_exists = true;
            }
            if file_name.ends_with("_down.sql") {
                down_exists = true;
            }
        }
        let _ = std::fs::remove_dir_all("./migrations/test");
        assert_eq!(count, 2);
        assert_eq!(up_exists, true);
        assert_eq!(down_exists, true);
    }
}
