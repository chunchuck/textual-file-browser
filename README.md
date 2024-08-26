# Terminal(Textual) File Browser
File browser in terminal built with [Textual](https://github.com/Textualize/textual). For personal use.

![Preview](./preview.png)

### Install
```
python -m venv tfb
source ./tfb/bin/activate
python -m pip install -r requirements.txt
```
### Run
```
python app.py
```
### Shortcuts
```
(ctrl+c) Quit
(q) Drive presets select

(enter) select folder / file
(r) Refresh selected folder

(:) Start typing command, (escape) Stop typing command
(1 - 9) Paste command presets
(f1) Paste highlighted folder/file path to command
(/) Start searching, (escape) Stop searching, (up)(down) next / previous match

(escape) go out to parent folder

(b) File browser window focus
(f) File preview window focus
(d) Data preview window focus
(w) Log window focus
```
### Support Filesystems (in theory)
See [universal-pathlib](https://github.com/fsspec/universal_pathlib?tab=readme-ov-file#currently-supported-filesystems-and-protocols). You might have to install extra libraries. Edit `DirectoryTreeApp.DRIVES` to add more filesystems.
