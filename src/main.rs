use anyhow::Result;
use clap::Parser;
use native_tls::{Certificate, TlsConnector};
use postgres::{Client, NoTls};
use postgres_native_tls::MakeTlsConnector;
use serde::Deserialize;
use sqlparser::dialect::PostgreSqlDialect;

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
        self.assert()?;
        let mut client = self.connect()?;
        client.execute(
            "
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version BIGINT PRIMARY KEY,
                dirty BOOLEAN DEFAULT FALSE
            )
        ",
            &[],
        )?;
        let mut last_version: i64 = 0;
        if let Some(row) = (client.query(
            "SELECT version, dirty FROM schema_migrations ORDER BY version DESC LIMIT 1",
            &[],
        )?)
        .into_iter()
        .next()
        {
            let version: i64 = row.get(0);
            let dirty: bool = row.get(1);
            if dirty {
                return Err(anyhow::anyhow!(
                    "last version is dirty. migration had failed previously"
                ));
            }
            last_version = version;
        }
        Ok((client, last_version))
    }

    fn dir(&self, parent: &std::path::Path) -> Result<std::path::PathBuf> {
        let mig_path = parent.join(&self.app);
        if mig_path.exists() && !mig_path.is_dir() {
            return Err(anyhow::anyhow!(format!("invalid path: {:?}", &mig_path)));
        }
        if !mig_path.exists() {
            std::fs::create_dir_all(&mig_path)?;
        }
        Ok(mig_path)
    }
}

struct Migrator {
    config: Config,
    dir: std::path::PathBuf,
    last_version: i64,
    client: Client,
    versions_up: Vec<i64>,
    versions_down: Vec<i64>,
    initialized: bool,
}

impl Migrator {
    fn new(mut config: Config, dir: std::path::PathBuf) -> Result<Self> {
        let dir = config.dir(&dir)?;
        let (client, last_version) = config.init()?;
        let mut m = Migrator {
            config,
            dir,
            last_version,
            client,
            versions_up: Vec::<i64>::new(),
            versions_down: Vec::<i64>::new(),
            initialized: false,
        };
        m.initialized = true;
        m.available_versions()?;
        Ok(m)
    }

    fn available_versions(&mut self) -> Result<()> {
        if !self.initialized {
            return Err(anyhow::anyhow!("Migrator not initialized"));
        }

        let reg = regex::Regex::new(r"^([1-9][0-9]*)_(up|down)\.sql$")?;
        let mut vup = Vec::<i64>::new();
        let mut vdown = Vec::<i64>::new();
        for f in std::fs::read_dir(&self.dir)? {
            let f = f?;
            let f = if let Some(v) = f.file_name().to_str() {
                String::from(v)
            } else {
                eprintln!("osstring to str failed");
                continue;
            };
            let caps = if let Some(v) = reg.captures(&f) {
                v
            } else {
                continue;
            };

            let version = match caps.get(1) {
                Some(v) => String::from(v.as_str()),
                None => {
                    continue;
                }
            };
            let direction = match caps.get(2) {
                Some(v) => String::from(v.as_str()),
                None => {
                    continue;
                }
            };

            match version.parse::<i64>() {
                Ok(v) => {
                    if &direction == "up" {
                        vup.push(v);
                    } else if &direction == "down" {
                        vdown.push(v);
                    }
                }
                Err(e) => {
                    eprintln!("{:?}", e);
                }
            }
        }
        self.versions_up = vup;
        self.versions_down = vdown;
        self.versions_up.sort();
        self.versions_down.sort();

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
        Ok(())
    }

    fn get_queries(&self, version: i64, direction: &str) -> Result<Vec<String>> {
        let mut result = Vec::<String>::new();

        let filename = self.dir.join(format!("{}_{}.sql", &version, &direction));
        if !filename.exists() {
            return Err(anyhow::anyhow!(format!(
                "migration: \"{}_{}.sql\" does not exist",
                &version, &direction
            )));
        }

        let f = match filename.to_str() {
            Some(v) => v.to_owned(),
            None => {
                return Err(anyhow::anyhow!("failed to get &str from PathBuf"));
            }
        };

        let s = std::fs::read_to_string(&f)?;
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, &s)?;
        for v in ast.iter() {
            result.push(v.to_string());
        }
        if direction == "up" {
            result.push(format!(
                "INSERT INTO schema_migrations(version) VALUES ({})",
                version
            ));
        } else if direction == "down" {
            result.push(format!(
                "DELETE FROM schema_migrations WHERE version = {}",
                version,
            ))
        }

        Ok(result)
    }

    fn run_migration(&mut self, version: i64, direction: String) -> Result<()> {
        eprintln!("run_migration called");
        let queries = self.get_queries(version, &direction)?;
        let mut t = self.client.transaction()?;
        for query in queries {
            t.batch_execute(&query)?;
        }
        t.commit()?;
        Ok(())
    }

    fn migrate_up_n(&mut self, n: usize, test: bool) -> Result<usize> {
        if self.versions_up.is_empty() {
            return Err(anyhow::anyhow!("no migrations found"));
        }

        let mut version = self.last_version;
        let mut versions = Vec::<i64>::new();
        let mut count = 0;
        for v in self.versions_up.iter() {
            if *v > version && count < n {
                versions.push(*v);
                version = *v;
                count += 1;
            }
        }

        for v in versions.iter() {
            if !test {
                match self.run_migration(*v, "up".to_owned()) {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("{}", e);
                        return Err(anyhow::anyhow!(format!(
                            "error running migration {}_up.sql",
                            *v
                        )));
                    }
                }
            }
            self.last_version = *v;
        }

        Ok(versions.len())
    }

    fn migrate_up(&mut self, test: bool) -> Result<usize> {
        if self.versions_up.is_empty() {
            return Err(anyhow::anyhow!("no migrations found"));
        }

        let mut version = self.last_version;
        let mut versions = Vec::<i64>::new();
        for v in self.versions_up.iter() {
            if *v > version {
                versions.push(*v);
                version = *v;
            }
        }

        for v in versions.iter() {
            if !test {
                match self.run_migration(*v, "up".to_owned()) {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("{}", e);
                        return Err(anyhow::anyhow!(format!(
                            "error running migration {}_up.sql",
                            *v
                        )));
                    }
                }
            }
            self.last_version = *v;
        }
        Ok(versions.len())
    }

    fn migrate_down_n(&mut self, n: usize, test: bool) -> Result<usize> {
        if self.versions_down.is_empty() {
            return Err(anyhow::anyhow!("no migrations found"));
        }

        if n == 0 {
            return Err(anyhow::anyhow!("0 steps requested"));
        }

        let mut version = self.last_version;
        let mut versions = Vec::<i64>::new();
        let mut count = 0;
        let mut index = 0;
        for (i, v) in self.versions_down.iter().rev().enumerate() {
            if *v <= version && count < n {
                versions.push(*v);
                version = *v;
                count += 1;
                index = self.versions_down.len() - 1 - i;
            }
        }
        eprintln!("index: {}", &index);

        for v in versions.iter() {
            self.last_version = *v;
            if !test {
                match self.run_migration(*v, "up".to_owned()) {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("{}", e);
                        return Err(anyhow::anyhow!(format!(
                            "error running migration {}_up.sql",
                            *v
                        )));
                    }
                }
            }
        }
        if index > 0 {
            if let Some(v) = self.versions_down.get(index - 1) {
                self.last_version = *v;
            }
        } else {
            self.last_version = 0;
        }

        Ok(versions.len())
    }

    fn migrate_down(&mut self, test: bool) -> Result<usize> {
        if self.versions_down.is_empty() {
            return Err(anyhow::anyhow!("no migrations found"));
        }

        let mut version = self.last_version;
        let mut versions = Vec::<i64>::new();
        for v in self.versions_down.iter().rev() {
            if *v <= version {
                versions.push(*v);
                version = *v;
            }
        }

        for v in versions.iter() {
            self.last_version = *v;
            if !test {
                match self.run_migration(*v, "up".to_owned()) {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("{}", e);
                        return Err(anyhow::anyhow!(format!(
                            "error running migration {}_up.sql",
                            *v
                        )));
                    }
                }
            }
        }
        self.last_version = 0;

        Ok(versions.len())
    }
}

#[derive(Debug, Parser)]
#[command(author,version,about,long_about=None)]
struct Args {
    #[arg(short, long, default_value = "./migrations")]
    migdir: String,
    #[arg(short, long)]
    config: String,
    #[arg(long)]
    upn: usize,
    #[arg(long)]
    up: bool,
    #[arg(long)]
    downn: usize,
    #[arg(long)]
    down: bool,
    #[arg(short, long)]
    new: bool,
    #[arg(short, long)]
    wizard: bool,
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
    println!("versions up:\n{:?}", &m.versions_up);
    println!("versions down:\n{:?}", &m.versions_down);

    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use std::sync::Once;
    static INIT: Once = Once::new();

    fn init() {
        INIT.call_once(|| {
            // Start dummy pg or cockroach server if needed
        });
    }

    fn test_config() -> Result<crate::Config> {
        let p = std::env::var("PGMIG_TEST_CONFIG")?;
        let s = std::fs::read_to_string(p)?;
        let c: crate::Config = toml::from_str(&s)?;
        Ok(c)
    }

    #[test]
    fn new_migration() {
        init();
        let config = test_config().unwrap();
        let mut m =
            crate::Migrator::new(config, std::path::PathBuf::from("./new_migrations")).unwrap();
        m.new_migration().unwrap();
        let files = std::fs::read_dir("./new_migrations/test").unwrap();
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
        let _ = std::fs::remove_dir_all("./new_migrations");
        assert_eq!(count, 2);
        assert!(up_exists);
        assert!(down_exists);
    }

    #[test]
    fn available_versions() {
        init();
        let config = test_config().unwrap();
        let mut m = crate::Migrator::new(config, std::path::PathBuf::from("./avvers")).unwrap();
        const N: usize = 5;
        for _ in 0..N {
            m.new_migration().unwrap();
        }
        let _ = std::fs::remove_dir_all("./avvers");
        assert_eq!(m.versions_up.len(), N);
        assert_eq!(m.versions_down.len(), N);
    }

    #[test]
    fn run_mig() {
        init();
        let config = test_config().unwrap();
        let mut m =
            crate::Migrator::new(config, std::path::PathBuf::from("./run_migrations")).unwrap();
        m.new_migration().unwrap();
        let version = *m.versions_up.last().unwrap();
        let f = m.dir.join(format!("{}_up.sql", version));
        std::fs::write(
            &f,
            b"
CREATE TABLE IF NOT EXISTS __data__(
    id INT PRIMARY KEY,
    name VARCHAR(20) DEFAULT ''
);
        CREATE TABLE IF NOT EXISTS __another__(
    id INT PRIMARY KEY,
    name VARCHAR(20) DEFAULT ''
);",
        )
        .unwrap();

        let fdown = m.dir.join(format!("{}_down.sql", version));
        std::fs::write(
            &fdown,
            b"
DROP TABLE IF EXISTS __data__;
DROP TABLE IF EXISTS __another__;",
        )
        .unwrap();

        m.run_migration(version, "up".to_owned()).unwrap();

        let vrows = m
            .client
            .query(
                r"SELECT version FROM schema_migrations WHERE version = $1",
                &[&version],
            )
            .unwrap();

        let rows = m.client.query(r"SHOW TABLES", &[]).unwrap();

        let mut count = 0;
        for row in rows.iter() {
            let v: &str = row.get(1);
            if v == "__another__" || v == "__data__" {
                count += 1;
            }
        }
        let mver: i64 = vrows.first().unwrap().get(0);
        assert_eq!(mver, version);
        assert_eq!(count, 2);

        m.run_migration(version, "down".to_owned()).unwrap();
        let vrows = m
            .client
            .query(
                r"SELECT version FROM schema_migrations WHERE version = $1",
                &[&version],
            )
            .unwrap();

        let rows = m.client.query(r"SHOW TABLES", &[]).unwrap();
        let mut count = 0;
        for row in rows.iter() {
            let v: &str = row.get(1);
            if v == "__another__" || v == "__data__" {
                count += 1;
            }
        }
        let _ = std::fs::remove_dir_all("./run_migrations");

        assert_eq!(vrows.len(), 0);
        assert_eq!(count, 0);
    }

    #[test]
    fn mig_up_n() {
        init();
        let config = test_config().unwrap();
        let mut m = crate::Migrator::new(config, std::path::PathBuf::from("./mig_up_n")).unwrap();
        const N: usize = 15;
        for _ in 0..N {
            m.new_migration().unwrap();
        }
        let v = *m.versions_up.get(10).unwrap();
        let n = m.migrate_up_n(11, true).unwrap(); // call with test true to not run migrations

        let _ = std::fs::remove_dir_all("./mig_up_n");

        assert_eq!(v, m.last_version);
        assert_eq!(n, 11);
    }
    #[test]
    fn mig_up() {
        init();
        let config = test_config().unwrap();
        let mut m = crate::Migrator::new(config, std::path::PathBuf::from("./mig_up")).unwrap();
        const N: usize = 15;
        for _ in 0..N {
            m.new_migration().unwrap();
        }
        let n = m.migrate_up(true).unwrap(); // call with test true to not run migrations

        let _ = std::fs::remove_dir_all("./mig_up");

        assert_eq!(Some(&m.last_version), m.versions_up.last());
        assert_eq!(n, N);
    }
    #[test]
    fn mig_down_n_gt_N() {
        init();
        let config = test_config().unwrap();
        let mut m =
            crate::Migrator::new(config, std::path::PathBuf::from("./mig_down_n_gt_N")).unwrap();
        const N: usize = 15;
        for _ in 0..N {
            m.new_migration().unwrap();
        }
        // n > N
        m.last_version = *m.versions_down.last().unwrap();
        let n = m.migrate_down_n(20, true).unwrap(); // call with test true to not run migrations

        let _ = std::fs::remove_dir_all("./mig_down_n_gt_N");

        assert_eq!(m.last_version, 0);
        assert_eq!(n, 15);
    }
    #[test]
    fn mig_down_n_lt_N() {
        init();
        let config = test_config().unwrap();
        let mut m =
            crate::Migrator::new(config, std::path::PathBuf::from("./mig_down_n_lt_N")).unwrap();
        const N: usize = 15;
        for _ in 0..N {
            m.new_migration().unwrap();
        }
        // n > N
        m.last_version = *m.versions_down.last().unwrap();
        let n = m.migrate_down_n(5, true).unwrap(); // call with test true to not run migrations

        let _ = std::fs::remove_dir_all("./mig_down_n_lt_N");

        assert_eq!(Some(&m.last_version), m.versions_down.get(9));
        assert_eq!(n, 5);
    }
    #[test]
    fn mig_down_n_not_from_end() {
        init();
        let config = test_config().unwrap();
        let mut m = crate::Migrator::new(
            config,
            std::path::PathBuf::from("./mig_down_n_not_from_end"),
        )
        .unwrap();
        const N: usize = 15;
        for _ in 0..N {
            m.new_migration().unwrap();
        }
        // n > N
        m.last_version = *m.versions_down.get(11).unwrap();
        let n = m.migrate_down_n(5, true).unwrap(); // call with test true to not run migrations

        let _ = std::fs::remove_dir_all("./mig_down_n_not_from_end");

        assert_eq!(Some(&m.last_version), m.versions_down.get(6));
        assert_eq!(n, 5);
    }
    #[test]
    fn mig_down_from_end() {
        init();
        let config = test_config().unwrap();
        let mut m =
            crate::Migrator::new(config, std::path::PathBuf::from("./mig_down_from_end")).unwrap();
        const N: usize = 15;
        for _ in 0..N {
            m.new_migration().unwrap();
        }
        m.last_version = *m.versions_up.last().unwrap();
        let n = m.migrate_down(true).unwrap(); // call with test true to not run migrations

        let _ = std::fs::remove_dir_all("./mig_down_from_end");

        assert_eq!(m.last_version, 0);
        assert_eq!(n, 15);
    }
    #[test]
    fn mig_down_from_not_end() {
        init();
        let config = test_config().unwrap();
        let mut m =
            crate::Migrator::new(config, std::path::PathBuf::from("./mig_down_from_not_end"))
                .unwrap();
        const N: usize = 15;
        for _ in 0..N {
            m.new_migration().unwrap();
        }
        m.last_version = *m.versions_up.get(11).unwrap();
        let n = m.migrate_down(true).unwrap(); // call with test true to not run migrations

        let _ = std::fs::remove_dir_all("./mig_down_from_not_end");

        assert_eq!(m.last_version, 0);
        assert_eq!(n, 12);
    }
}
