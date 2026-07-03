use std::fs;
use std::io::Write;
use std::path::PathBuf;

use tempfile::tempdir;

#[cfg(feature = "media-ffi")]
use super::convert_wav_bytes_to_mp3_with_backend;
use super::{
    convert_m2v_bytes_to_mp4_with_backend, convert_m2v_to_mp4_with_backend,
    convert_usm_to_mp4_with_backend, FrameRate,
};
use crate::core::config::MediaBackend;
use crate::core::config::RetryConfig;

#[cfg(not(target_os = "windows"))]
const SCRIPT_EXT: &str = "sh";
#[cfg(target_os = "windows")]
const SCRIPT_EXT: &str = "bat";

fn write_executable_script(path: &std::path::Path, script: impl AsRef<[u8]>) {
    let mut file = fs::File::create(path).unwrap();
    file.write_all(script.as_ref()).unwrap();
    file.sync_all().unwrap();
    drop(file);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }
}

fn get_fake_ffmpeg_script(input_log: Option<&str>) -> String {
    if cfg!(target_os = "windows") {
        if let Some(log_path) = input_log {
            format!(
                "@echo off\n\
                 setlocal enabledelayedexpansion\n\
                 set \"input=\"\n\
                 set \"out=\"\n\
                 set \"prev=\"\n\
                 for %%a in (%*) do (\n\
                   if \"!prev!\"==\"-i\" (\n\
                     set \"input=%%~a\"\n\
                   )\n\
                   set \"out=%%~a\"\n\
                   set \"prev=%%~a\"\n\
                 )\n\
                 <nul set /p=\"!input!\" > \"{}\"\n\
                 copy /y nul \"!out!\" >nul\n",
                log_path
            )
        } else {
            "@echo off\n\
             set \"out=\"\n\
             for %%a in (%*) do (\n\
               set \"out=%%~a\"\n\
             )\n\
             copy /y nul \"%out%\" >nul\n"
                .to_string()
        }
    } else {
        if let Some(log_path) = input_log {
            format!(
                "#!/bin/sh\n\
                 set -eu\n\
                 input=\"\"\n\
                 out=\"\"\n\
                 prev=\"\"\n\
                 for arg in \"$@\"; do\n\
                   if [ \"$prev\" = \"-i\" ]; then input=\"$arg\"; fi\n\
                   out=\"$arg\"\n\
                   prev=\"$arg\"\n\
                 done\n\
                 printf '%s' \"$input\" > \"{}\"\n\
                 : > \"$out\"\n",
                log_path
            )
        } else {
            "#!/bin/sh\n\
             set -eu\n\
             out=\"\"\n\
             for arg in \"$@\"; do\n\
               out=\"$arg\"\n\
             done\n\
             : > \"$out\"\n"
                .to_string()
        }
    }
}

fn get_fake_ffmpeg_retry_script(marker_path: &str) -> String {
    if cfg!(target_os = "windows") {
        format!(
            "@echo off\n\
             if not exist \"{}\" (\n\
               echo first > \"{}\"\n\
               echo transient 1>&2\n\
               exit /b 1\n\
             )\n\
             set \"out=\"\n\
             for %%a in (%*) do (\n\
               set \"out=%%~a\"\n\
             )\n\
             copy /y nul \"%out%\" >nul\n",
            marker_path, marker_path
        )
    } else {
        format!(
            "#!/bin/sh\n\
             set -eu\n\
             MARKER=\"{}\"\n\
             if [ ! -f \"$MARKER\" ]; then\n\
               echo first > \"$MARKER\"\n\
               echo transient >&2\n\
               exit 1\n\
             fi\n\
             out=\"\"\n\
             for arg in \"$@\"; do\n\
               out=\"$arg\"\n\
             done\n\
             : > \"$out\"\n",
            marker_path
        )
    }
}

#[test]
fn frame_rate_formats_like_go_helper() {
    assert_eq!(
        FrameRate {
            numerator: 30000,
            denominator: 1001
        }
        .to_string(),
        "30000/1001"
    );
    assert_eq!(
        FrameRate {
            numerator: 60,
            denominator: 1
        }
        .to_string(),
        "60"
    );
}

#[test]
fn convert_usm_to_mp4_builds_ffmpeg_command() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("sample.usm");
    let output = dir.path().join("sample.mp4");
    let script_path = dir.path().join(format!("fake_ffmpeg.{}", SCRIPT_EXT));
    fs::write(&input, b"dummy").unwrap();
    write_executable_script(&script_path, get_fake_ffmpeg_script(None));

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(convert_usm_to_mp4_with_backend(
            &input,
            &output,
            &script_path.to_string_lossy(),
            MediaBackend::Cli,
            &RetryConfig {
                attempts: 1,
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
            },
        ))
        .unwrap();

    assert!(output.exists());
}

#[test]
fn convert_m2v_to_mp4_removes_original_when_requested() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("sample.m2v");
    let output = dir.path().join("sample.mp4");
    let script_path = dir.path().join(format!("fake_ffmpeg.{}", SCRIPT_EXT));
    fs::write(&input, b"dummy").unwrap();
    write_executable_script(&script_path, get_fake_ffmpeg_script(None));

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(convert_m2v_to_mp4_with_backend(
            &input,
            &output,
            true,
            &script_path.to_string_lossy(),
            MediaBackend::Cli,
            Some(FrameRate {
                numerator: 30000,
                denominator: 1001,
            }),
            &RetryConfig {
                attempts: 1,
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
            },
        ))
        .unwrap();

    assert!(!input.exists());
    assert!(output.exists());
}

#[test]
fn convert_usm_to_mp4_retries_after_command_failure() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("sample.usm");
    let output = dir.path().join("sample.mp4");
    let script_path = dir.path().join(format!("fake_ffmpeg_retry.{}", SCRIPT_EXT));
    let marker_path = dir.path().join("attempts.txt");
    fs::write(&input, b"dummy").unwrap();
    write_executable_script(
        &script_path,
        get_fake_ffmpeg_retry_script(&marker_path.to_string_lossy()),
    );

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(convert_usm_to_mp4_with_backend(
            &input,
            &output,
            &script_path.to_string_lossy(),
            MediaBackend::Cli,
            &RetryConfig {
                attempts: 2,
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
            },
        ))
        .unwrap();

    assert!(marker_path.exists());
    assert!(output.exists());
}

#[test]
fn auto_backend_falls_back_to_cli() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("sample.usm");
    let output = dir.path().join("sample.mp4");
    let script_path = dir.path().join(format!("fake_ffmpeg.{}", SCRIPT_EXT));
    fs::write(&input, b"dummy").unwrap();
    write_executable_script(&script_path, get_fake_ffmpeg_script(None));

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(convert_usm_to_mp4_with_backend(
            &input,
            &output,
            &script_path.to_string_lossy(),
            MediaBackend::Auto,
            &RetryConfig {
                attempts: 1,
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
            },
        ))
        .unwrap();

    assert!(output.exists());
}

#[cfg(feature = "media-ffi")]
#[test]
fn ffi_usm_to_mp4_handles_real_sample_when_available() {
    let Some(sample) = std::env::var_os("HARUKI_USM_SAMPLE").map(PathBuf::from) else {
        return;
    };
    if !sample.exists() {
        return;
    }

    let dir = tempdir().unwrap();
    let output = dir.path().join("sample.mp4");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(convert_usm_to_mp4_with_backend(
            &sample,
            &output,
            "ffmpeg",
            MediaBackend::Ffi,
            &RetryConfig {
                attempts: 1,
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
            },
        ))
        .unwrap();

    assert!(output.exists());
    assert!(fs::metadata(&output).unwrap().len() > 0);
    if let Some(copy_to) = std::env::var_os("HARUKI_USM_OUTPUT").map(PathBuf::from) {
        fs::copy(&output, copy_to).unwrap();
    }
}

#[test]
fn cli_bytes_input_uses_system_temp_dir() {
    let dir = tempdir().unwrap();
    let output_dir = dir.path().join("exports");
    fs::create_dir_all(&output_dir).unwrap();
    let output = output_dir.join("sample.mp4");
    let script_path = dir.path().join(format!("fake_ffmpeg.{}", SCRIPT_EXT));
    let input_log = dir.path().join("input_path.txt");
    write_executable_script(
        &script_path,
        get_fake_ffmpeg_script(Some(&input_log.to_string_lossy())),
    );

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(convert_m2v_bytes_to_mp4_with_backend(
            b"dummy m2v",
            &output,
            &script_path.to_string_lossy(),
            MediaBackend::Cli,
            None,
            &RetryConfig {
                attempts: 1,
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
            },
        ))
        .unwrap();

    let temp_input = PathBuf::from(fs::read_to_string(&input_log).unwrap());
    assert!(output.exists());
    assert!(!temp_input.exists());
    assert!(!temp_input.starts_with(&output_dir));
}

#[cfg(feature = "media-ffi")]
#[test]
fn ffi_backend_transcodes_wav_bytes_to_mp3() {
    let dir = tempdir().unwrap();
    let output = dir.path().join("sample.mp3");
    let wav = test_wav_bytes();

    convert_wav_bytes_to_mp3_with_backend(
        &wav,
        &output,
        "ffmpeg",
        MediaBackend::Ffi,
        &RetryConfig {
            attempts: 1,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        },
    )
    .unwrap();

    assert!(fs::metadata(output).unwrap().len() > 0);
}

#[cfg(feature = "media-ffi")]
fn test_wav_bytes() -> Vec<u8> {
    let sample_rate = 44_100_u32;
    let channels = 1_u16;
    let bits_per_sample = 16_u16;
    let samples = sample_rate / 10;
    let block_align = channels * bits_per_sample / 8;
    let byte_rate = sample_rate * u32::from(block_align);
    let data_len = samples * u32::from(block_align);
    let mut wav = Vec::with_capacity(44 + data_len as usize);

    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36 + data_len).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16_u32.to_le_bytes());
    wav.extend_from_slice(&1_u16.to_le_bytes());
    wav.extend_from_slice(&channels.to_le_bytes());
    wav.extend_from_slice(&sample_rate.to_le_bytes());
    wav.extend_from_slice(&byte_rate.to_le_bytes());
    wav.extend_from_slice(&block_align.to_le_bytes());
    wav.extend_from_slice(&bits_per_sample.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_len.to_le_bytes());

    for index in 0..samples {
        let t = index as f32 / sample_rate as f32;
        let sample = (t * 440.0 * std::f32::consts::TAU).sin();
        let value = (sample * i16::MAX as f32 * 0.25) as i16;
        wav.extend_from_slice(&value.to_le_bytes());
    }
    wav
}
