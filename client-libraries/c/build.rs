use cbindgen::{
    Braces, Config, CythonConfig, DocumentationStyle, EnumConfig, ExportConfig, Language,
    RenameRule,
};
use std::{
    env, fs,
    io::Write,
    path::{Path, PathBuf},
};

const CAPI_OUT_DIR_ENV_VAR: &str = "MODALITY_SDK_CAPI_OUT_DIR";
const CYTHON_OUT_DIR_ENV_VAR: &str = "MODALITY_SDK_CYTHON_OUT_DIR";

fn main() {
    let out_dir = env::var(CAPI_OUT_DIR_ENV_VAR)
        .or_else(|_| env::var("OUT_DIR"))
        .expect("Failed to determine output artifact directory");

    let cython_dir = env::var(CYTHON_OUT_DIR_ENV_VAR)
        .map(PathBuf::from)
        .unwrap_or_else(|_| Path::new(&out_dir).join("cython"));
    fs::create_dir_all(&cython_dir).unwrap();

    let include_dir = Path::new(&out_dir).join("include").join("modality");
    fs::create_dir_all(&include_dir).unwrap();

    // Generate packaging helpers
    let version_major = env::var("CARGO_PKG_VERSION_MAJOR").unwrap();
    let deb_dir = Path::new(&out_dir).join("package");
    fs::create_dir_all(&deb_dir).unwrap();
    fs::write(
        deb_dir.join("provides"),
        format!("libmodality{version_major}"),
    )
    .unwrap();
    fs::write(
        deb_dir.join("soname"),
        format!("libmodality.so.{version_major}"),
    )
    .unwrap();
    fs::write(
        deb_dir.join("shlibs"),
        format!("libmodality {version_major} libmodality{version_major}"),
    )
    .unwrap();

    let common_cfg = Config {
        braces: Braces::NextLine,
        header: Some("/* This file is generated automatically, do not modify. */".to_string()),
        tab_width: 4,
        documentation: true,
        documentation_style: DocumentationStyle::Doxy,
        usize_is_size_t: true,
        enumeration: EnumConfig {
            rename_variants: RenameRule::QualifiedScreamingSnakeCase,
            ..Default::default()
        },
        export: ExportConfig {
            prefix: Some("modality_".to_string()),
            include: vec!["error".to_string()],
            ..Default::default()
        },
        ..Default::default()
    };

    generate_c_headers(&common_cfg, &include_dir);
    generate_cpp_headers(&common_cfg, &include_dir);
    generate_cython_bindings(&common_cfg, &cython_dir);

    // So we can get a valid SONAME without changing the package name
    std::env::set_var("CARGO_PKG_NAME", "modality");
    cdylib_link_lines::metabuild();
    std::env::set_var("CARGO_PKG_NAME", env!("CARGO_PKG_NAME"));

    println!("cargo:rerun-if-env-changed={CAPI_OUT_DIR_ENV_VAR}");
    println!("cargo:rerun-if-env-changed={CYTHON_OUT_DIR_ENV_VAR}");
}

fn generate_c_headers(common_cfg: &Config, include_dir: &Path) {
    let mut cfg = common_cfg.clone();

    cfg.language = Language::C;
    cfg.cpp_compat = false;
    cfg.no_includes = true;

    cbindgen::Builder::new()
        .with_config(cfg.clone())
        .with_no_includes()
        .with_include_guard("MODALITY_ERROR_H")
        .with_src("src/error.rs")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(include_dir.join("error.h"));

    cbindgen::Builder::new()
        .with_config(cfg.clone())
        .with_include_guard("MODALITY_TRACING_SUBSCRIBER_H")
        .with_src("src/tracing.rs")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(include_dir.join("tracing_subscriber.h"));

    cbindgen::Builder::new()
        .with_config(cfg.clone())
        .with_no_includes()
        .with_include_guard("MODALITY_RUNTIME_H")
        .with_src("src/rt.rs")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(include_dir.join("runtime.h"));

    cbindgen::Builder::new()
        .with_config(cfg.clone())
        .with_sys_include("stdint.h")
        .with_sys_include("stdbool.h")
        .with_include_guard("MODALITY_TYPES_H")
        .with_src("src/types.rs")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(include_dir.join("types.h"));

    cbindgen::Builder::new()
        .with_config(cfg.clone())
        .with_sys_include("stdint.h")
        .with_sys_include("stdbool.h")
        .with_include("modality/types.h")
        .with_include("modality/runtime.h")
        .with_include_guard("MODALITY_INGEST_CLIENT_H")
        .with_src("src/ingest/client.rs")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(include_dir.join("ingest_client.h"));

    cbindgen::Builder::new()
        .with_config(cfg.clone())
        .with_sys_include("stdint.h")
        .with_sys_include("stdbool.h")
        .with_include("modality/types.h")
        .with_include_guard("MODALITY_MUTATOR_INTERFACE_H")
        .with_src("src/mutation/interface.rs")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(include_dir.join("mutator_interface.h"));

    cbindgen::Builder::new()
        .with_config(cfg)
        .with_sys_include("stdint.h")
        .with_sys_include("stdbool.h")
        .with_include("modality/types.h")
        .with_include("modality/runtime.h")
        .with_include("modality/mutator_interface.h")
        .with_include_guard("MODALITY_MUTATOR_HTTP_SERVER_H")
        .with_src("src/mutation/http_server.rs")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(include_dir.join("mutator_http_server.h"));
}

fn generate_cpp_headers(common_cfg: &Config, include_dir: &Path) {
    let mut cfg = common_cfg.clone();

    cfg.language = Language::Cxx;
    cfg.cpp_compat = true;
    cfg.namespace = Some("modality".to_string());
    cfg.no_includes = false;

    cbindgen::Builder::new()
        .with_config(cfg.clone())
        .with_no_includes()
        .with_include_guard("MODALITY_ERROR_HPP")
        .with_src("src/error.rs")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(include_dir.join("error.hpp"));

    cbindgen::Builder::new()
        .with_config(cfg.clone())
        .with_no_includes()
        .with_include_guard("MODALITY_TRACING_SUBSCRIBER_HPP")
        .with_src("src/tracing.rs")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(include_dir.join("tracing_subscriber.hpp"));

    cbindgen::Builder::new()
        .with_config(cfg.clone())
        .with_no_includes()
        .with_include_guard("MODALITY_RUNTIME_HPP")
        .with_src("src/rt.rs")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(include_dir.join("runtime.hpp"));

    cbindgen::Builder::new()
        .with_config(cfg.clone())
        .with_include_guard("MODALITY_TYPES_HPP")
        .with_src("src/types.rs")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(include_dir.join("types.hpp"));

    cbindgen::Builder::new()
        .with_config(cfg.clone())
        .with_include("modality/types.hpp")
        .with_include("modality/runtime.hpp")
        .with_include_guard("MODALITY_INGEST_CLIENT_HPP")
        .with_src("src/ingest/client.rs")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(include_dir.join("ingest_client.hpp"));

    cbindgen::Builder::new()
        .with_config(cfg.clone())
        .with_include("modality/types.hpp")
        .with_include_guard("MODALITY_MUTATOR_INTERFACE_HPP")
        .with_src("src/mutation/interface.rs")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(include_dir.join("mutator_interface.hpp"));

    cbindgen::Builder::new()
        .with_config(cfg)
        .with_include("modality/types.hpp")
        .with_include("modality/runtime.hpp")
        .with_include("modality/mutator_interface.hpp")
        .with_include_guard("MODALITY_MUTATOR_HTTP_SERVER_HPP")
        .with_src("src/mutation/http_server.rs")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(include_dir.join("mutator_http_server.hpp"));
}

fn generate_cython_bindings(common_cfg: &Config, out_dir: &Path) {
    let pyx_path = out_dir.join("modality.pyx");
    let wrapper_path = Path::new("cython").join("wrapper.pyx");
    let mut cfg = common_cfg.clone();

    cfg.language = Language::Cython;
    cfg.namespace = Some("modality".to_string());
    cfg.no_includes = false;
    cfg.header = Some("# This file is generated automatically, do not modify.".to_string());
    cfg.cython = CythonConfig {
        header: Some("'modality_cython_includes.h'".to_owned()),
        ..Default::default()
    };

    cbindgen::Builder::new()
        .with_config(cfg)
        .with_src("src/error.rs")
        .with_src("src/tracing.rs")
        .with_src("src/rt.rs")
        .with_src("src/types.rs")
        .with_src("src/ingest/client.rs")
        .with_src("src/mutation/interface.rs")
        .with_src("src/mutation/http_server.rs")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(&pyx_path);

    let mut f = fs::OpenOptions::new().append(true).open(pyx_path).unwrap();
    f.write_all(fs::read_to_string(&wrapper_path).unwrap().as_bytes())
        .unwrap();

    println!("cargo:rerun-if-changed={}", wrapper_path.display());
    println!("cargo:rerun-if-changed=setup.py");
}
