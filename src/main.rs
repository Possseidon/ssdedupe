mod scan;
mod utils;

use std::{
    convert::identity,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    thread::{self, JoinHandle, available_parallelism},
};

use eframe::storage_dir;
use egui::{
    CentralPanel, CollapsingHeader, Grid, NumExt, ScrollArea, TextEdit, TopBottomPanel, Ui, vec2,
};
use humansize::{BINARY, FormatSize, FormatSizeOptions};

use crate::{
    scan::{Entry, EntryKind, ScanState},
    utils::TryJoin,
};

const APP_NAME: &str = "SSDeDupe";
const DRIVE_EXTENSION: &str = ".fsinfo";

const SIZE_FORMAT: FormatSizeOptions = BINARY;

fn main() -> eframe::Result {
    // keep one thread for UI
    rayon::ThreadPoolBuilder::new()
        .num_threads((available_parallelism().unwrap().get() - 1).at_least(1))
        .build_global()
        .unwrap();

    let mut storage_dir = storage_dir(APP_NAME).expect("disk storage should be supported");
    assert!(storage_dir.pop());
    let drives_dir = storage_dir.join("drives");
    fs::create_dir_all(&drives_dir).unwrap();

    let mut drives = drives_dir
        .read_dir()
        .unwrap()
        .map(|dir_entry| {
            let dir_entry = dir_entry.unwrap();
            let name = dir_entry
                .path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .strip_suffix(DRIVE_EXTENSION)
                .unwrap()
                .to_string();
            Drive::new(name, DriveState::load(&dir_entry.path()))
        })
        .collect::<Vec<_>>();

    let drive_path = move |name: &str| drives_dir.join(format!("{name}{DRIVE_EXTENSION}"));

    let mut select_drive = None;
    let mut update_duplicates = false;
    let mut redundant_bytes = 0;
    let mut duplicates = Vec::new();

    eframe::run_simple_native(APP_NAME, Default::default(), move |ctx, _frame| {
        TopBottomPanel::top("drives")
            .resizable(true)
            .min_height(0.0)
            .show(ctx, |ui| {
                ui.vertical_centered(|ui| {
                    if ui
                        .add_enabled(
                            select_drive.is_none(),
                            egui::Button::new("Select Drive or Folder to Scan..."),
                        )
                        .clicked()
                    {
                        let ctx = ctx.clone();
                        select_drive = Some(thread::spawn(move || {
                            let folder = rfd::FileDialog::new().pick_folder();
                            // technically not necessary since user interaction is more or less
                            // implied
                            ctx.request_repaint();
                            folder
                        }));
                    }
                });

                if let Some(selected_drive) = select_drive.try_join() {
                    if let Some(path) = selected_drive.expect("drive selection shouldn't panic") {
                        drives.push(Drive::new(
                            path.file_name()
                                .map_or_else(|| "new drive".into(), |x| x.to_string_lossy().into()),
                            DriveState::scan(path),
                        ));
                    } else {
                        // user cancelled the dialog
                    }
                }

                ScrollArea::vertical().show(ui, |ui| {
                    ui.set_width(ui.available_width());
                    Grid::new("drives")
                        .min_col_width(0.0)
                        .striped(true)
                        .show(ui, |ui| {
                            drives.retain_mut(|drive| {
                                match &drive.state {
                                    DriveState::Scanning { state, .. } => {
                                        if ui.button("❌").clicked() {
                                            state.cancel();
                                        }
                                    }
                                    DriveState::Done { .. } => {
                                        if ui.button("🗑").clicked()
                                            && fs::remove_file(drive_path(&drive.name)).is_ok()
                                        {
                                            return false;
                                        }
                                    }
                                }

                                let name_edit = ui.add_sized(
                                    vec2(200.0, ui.spacing().interact_size.y),
                                    TextEdit::singleline(&mut drive.edit_name),
                                );
                                if name_edit.lost_focus() && drive.edit_name != drive.name {
                                    if !drive.edit_name.is_empty() && !drive.state.is_done()
                                        || (drive_path(&drive.edit_name)
                                            .try_exists()
                                            .is_ok_and(|exists| !exists)
                                            && fs::rename(
                                                drive_path(&drive.name),
                                                drive_path(&drive.edit_name),
                                            )
                                            .is_ok())
                                    {
                                        drive.name = drive.edit_name.clone();
                                    } else {
                                        drive.edit_name = drive.name.clone();
                                    }
                                }

                                match &mut drive.state {
                                    DriveState::Scanning { state, join_handle } => {
                                        dirs_files_bytes(
                                            ui,
                                            state.bytes(),
                                            state.dirs(),
                                            state.files(),
                                        );

                                        ui.spinner();

                                        if let Some((error, extra)) = state.last_error_plus() {
                                            ui.colored_label(
                                                ui.visuals().warn_fg_color,
                                                format!("{error} (+{extra})"),
                                            );
                                        }

                                        if let Some(new_entry) = join_handle.try_join() {
                                            let error_log = state.clone_error_log();
                                            let name = drive.name.clone();
                                            let mut index = 1;
                                            while drive_path(&drive.name)
                                                .try_exists()
                                                .ok()
                                                .is_none_or(identity)
                                            {
                                                drive.name = format!("{name} ({index})");
                                                index += 1;
                                            }

                                            if !name_edit.has_focus() {
                                                drive.edit_name = drive.name.clone();
                                            }

                                            drive.state = DriveState::save(
                                                &drive_path(&drive.name),
                                                new_entry.unwrap_or_default(),
                                                error_log,
                                            );
                                        }
                                    }
                                    DriveState::Done { entry, error_log } => {
                                        if let Some((entry, enabled)) = entry {
                                            dirs_files_bytes(
                                                ui,
                                                entry.info().bytes,
                                                entry.dirs(),
                                                entry.files(),
                                            );

                                            if ui.checkbox(enabled, "").clicked() {
                                                update_duplicates = true;
                                            }
                                        }

                                        if !error_log.is_empty() {
                                            ui.label(format!("{} Errors", error_log.len()))
                                                .on_hover_ui(|ui| {
                                                    for message in &*error_log {
                                                        ui.label(message);
                                                    }
                                                });
                                        }
                                    }
                                }

                                ui.end_row();

                                true
                            });
                        });
                });
            });

        if update_duplicates {
            update_duplicates = false;

            let entry = Entry::dir(
                drives
                    .iter()
                    .filter_map(|drive| {
                        if let DriveState::Done {
                            entry: Some((entry, true)),
                            ..
                        } = &drive.state
                        {
                            Some(((&drive.name).into(), entry.clone()))
                        } else {
                            None
                        }
                    })
                    .collect(),
            );

            let unfiltered_duplicates = entry.unfiltered_duplicates();
            redundant_bytes = Entry::redundant_bytes(&unfiltered_duplicates);
            duplicates = Entry::filter_duplicates_by_prefix(unfiltered_duplicates)
                .into_iter()
                .map(|(info, paths)| (info.bytes * (paths.len() as u64 - 1), info, paths))
                .collect();
            duplicates.sort_unstable_by_key(|(redundant_bytes, info, paths)| {
                (*redundant_bytes, info.kind, paths.len())
            });
        }

        CentralPanel::default().show(ctx, |ui| {
            let bytes = redundant_bytes.format_size(SIZE_FORMAT);
            ui.heading(format!("Duplicates ({bytes} redundant)"));

            ScrollArea::vertical().show(ui, |ui| {
                ui.set_width(ui.available_width());
                for (redundant_bytes, info, paths) in duplicates.iter().rev() {
                    let redundant_bytes = redundant_bytes.format_size(SIZE_FORMAT);
                    let count = paths.len();
                    let bytes = info.bytes.format_size(SIZE_FORMAT);
                    let kind = match info.kind {
                        EntryKind::Dir => "Directories",
                        EntryKind::File => "Files",
                    };
                    CollapsingHeader::new(format!(
                        "{redundant_bytes} redundant across {count} {kind} ({bytes} each)"
                    ))
                    .id_salt(info)
                    .show(ui, |ui| {
                        for path in paths {
                            ui.label(path.to_string_lossy());
                        }
                    });
                }
            });
        });
    })
}

fn dirs_files_bytes(ui: &mut Ui, bytes: u64, dirs: u64, files: u64) {
    ui.label(format!("{dirs} dirs"));
    ui.label(format!("{files} files"));
    ui.label(bytes.format_size(SIZE_FORMAT));
}

struct Drive {
    name: String,
    edit_name: String,
    state: DriveState,
}

impl Drive {
    fn new(name: String, state: DriveState) -> Drive {
        Drive {
            name: name.clone(),
            edit_name: name,
            state,
        }
    }
}

enum DriveState {
    Scanning {
        state: Arc<ScanState>,
        join_handle: Option<JoinHandle<Option<Entry>>>,
    },
    Done {
        entry: Option<(Entry, bool)>,
        error_log: Vec<String>,
    },
}

impl DriveState {
    fn save(path: &Path, entry: Option<Entry>, mut error_log: Vec<String>) -> Self {
        if let Some(entry) = &entry {
            match postcard::to_allocvec(entry) {
                Ok(data) => {
                    if let Err(error) = fs::write(path, &data) {
                        error_log.push(error.to_string())
                    }
                }
                Err(error) => {
                    error_log.push(error.to_string());
                }
            };
        }

        Self::Done {
            entry: entry.map(|entry| (entry, false)),
            error_log,
        }
    }

    fn load(path: &Path) -> Self {
        Self::Done {
            entry: Some((
                postcard::from_bytes(&fs::read(path).unwrap()).unwrap(),
                false,
            )),
            error_log: Default::default(),
        }
    }

    fn scan(path: PathBuf) -> DriveState {
        let state = ScanState::new();
        let join_handle = Some(thread::spawn({
            let state = state.clone();
            move || Entry::scan(&path, &state)
        }));

        Self::Scanning { state, join_handle }
    }

    fn is_done(&self) -> bool {
        matches!(self, Self::Done { .. })
    }
}
