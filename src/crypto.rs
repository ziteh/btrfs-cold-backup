use age::secrecy::SecretString;
use age::x25519;
use anyhow::{Result, anyhow};
use std::io::Read;
use std::path::Path;
use tokio::task;

pub struct AgeKeyPair {
    pub public_key: String,
    pub private_key: SecretString,
}

pub fn write_private_key(path: &std::path::Path, key: &str) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| anyhow!("failed to create {:?}: {e}", path))?;
    file.write_all(key.as_bytes())
        .map_err(|e| anyhow!("failed to write private key to {:?}: {e}", path))
}

pub fn generate_key_pair() -> AgeKeyPair {
    let identity = age::x25519::Identity::generate();
    AgeKeyPair {
        public_key: identity.to_public().to_string(),
        private_key: identity.to_string(),
    }
}

pub async fn encrypt_file(input: &Path, output: &Path, public_key_str: &str) -> Result<()> {
    let public_key = public_key_str.to_string();
    let input_buf = input.to_path_buf();
    let output_buf = output.to_path_buf();

    task::spawn_blocking(move || -> Result<()> {
        let recipient: x25519::Recipient = public_key
            .parse()
            .map_err(|e| anyhow!("invalid age public key: {e}"))?;

        let encryptor =
            age::Encryptor::with_recipients(std::iter::once(&recipient as &dyn age::Recipient))
                .map_err(|e| anyhow!("failed to create encryptor: {e}"))?;

        let mut input_file = std::io::BufReader::new(std::fs::File::open(&input_buf)?);
        let mut output_file = std::fs::File::create(&output_buf)?;

        let mut writer = encryptor
            .wrap_output(&mut output_file)
            .map_err(|e| anyhow!("failed to initialize encryption: {e}"))?;
        std::io::copy(&mut input_file, &mut writer)?;
        writer
            .finish()
            .map_err(|e| anyhow!("failed to finalize encryption: {e}"))?;

        Ok(())
    })
    .await?
}

pub async fn decrypt_file(input: &Path, output: &Path, private_key_str: &str) -> Result<()> {
    let private_key = private_key_str.to_string();
    let input_buf = input.to_path_buf();
    let output_buf = output.to_path_buf();

    task::spawn_blocking(move || -> Result<()> {
        let identity: x25519::Identity = private_key
            .parse()
            .map_err(|e| anyhow!("invalid age private key: {e}"))?;

        let input_file = std::io::BufReader::new(std::fs::File::open(&input_buf)?);

        let decryptor = age::Decryptor::new_buffered(input_file)
            .map_err(|e| anyhow!("failed to create decryptor: {e}"))?;

        let mut reader = decryptor
            .decrypt(std::iter::once(&identity as &dyn age::Identity))
            .map_err(|e| anyhow!("decryption failed: {e}"))?;
        let mut output_file = std::fs::File::create(&output_buf)?;
        std::io::copy(&mut reader, &mut output_file)?;

        Ok(())
    })
    .await?
}

pub async fn blake3_file(path: &Path) -> Result<String> {
    let path = path.to_path_buf();

    task::spawn_blocking(move || -> Result<String> {
        let mut file = std::fs::File::open(&path)?;
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; 65536];

        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }

        Ok(hasher.finalize().to_hex().to_string())
    })
    .await?
}
