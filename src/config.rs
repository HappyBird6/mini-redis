pub struct ServerConfig {
    pub address: String,
    pub max_connections: usize,
}

pub fn from_env() -> Result<ServerConfig, ConfigError> {
    // std::env::var("키값")으로 읽어옴

    let address = match std::env::var("MINI_REDIS_ADDRESS") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "0.0.0.0:6379".to_owned(),
        Err(_) => return Err(ConfigError::InvalidAddress),
    };
    if address.trim().is_empty() {
        return Err(ConfigError::InvalidAddress);
    }
    let max_connections = match std::env::var("MINI_REDIS_MAX_CONNECTIONS") {
        Ok(value) => value
            .parse::<usize>()
            .map_err(|_| ConfigError::InvalidMaxConnections)?,
        Err(std::env::VarError::NotPresent) => 100,
        Err(_) => return Err(ConfigError::InvalidMaxConnections),
    };
    if max_connections == 0 || max_connections > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(ConfigError::InvalidMaxConnections);
    }

    Ok(ServerConfig {
        address,
        max_connections,
    })
}
#[derive(Debug)]
pub enum ConfigError {
    InvalidAddress,
    InvalidMaxConnections,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_environment_configuration() {
        const CASE_ENV: &str = "MINI_REDIS_CONFIG_TEST_CASE";
        if let Ok(case) = std::env::var(CASE_ENV) {
            match case.as_str() {
                "default" | "custom" => {
                    let config = from_env().unwrap();
                    let (address, limit) = if case == "default" {
                        ("0.0.0.0:6379", 100)
                    } else {
                        ("127.0.0.1:6380", 2)
                    };
                    assert_eq!(config.address, address);
                    assert_eq!(config.max_connections, limit);
                }
                "address" => assert!(matches!(from_env(), Err(ConfigError::InvalidAddress))),
                "limit" => assert!(matches!(
                    from_env(),
                    Err(ConfigError::InvalidMaxConnections)
                )),
                _ => panic!("unknown case"),
            }
            return;
        }

        // 환경변수 변경은 별도 프로세스에 한정하여 병렬 테스트 간 간섭을 막는다.
        let excessive = (tokio::sync::Semaphore::MAX_PERMITS + 1).to_string();
        for (case, address, limit) in [
            ("default", None, None),
            ("custom", Some("127.0.0.1:6380"), Some("2")),
            ("address", Some("  "), None),
            ("limit", None, Some("0")),
            ("limit", None, Some("-1")),
            ("limit", None, Some("abc")),
            ("limit", None, Some(excessive.as_str())),
        ] {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "config::tests::validates_environment_configuration",
                ])
                .env(CASE_ENV, case)
                .env_remove("MINI_REDIS_ADDRESS")
                .env_remove("MINI_REDIS_MAX_CONNECTIONS");
            if let Some(address) = address {
                command.env("MINI_REDIS_ADDRESS", address);
            }
            if let Some(limit) = limit {
                command.env("MINI_REDIS_MAX_CONNECTIONS", limit);
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}
