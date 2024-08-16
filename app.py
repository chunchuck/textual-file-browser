from textual import on
from textual.app import App, ComposeResult
from textual.containers import Horizontal, VerticalScroll, Vertical, HorizontalScroll
from textual.widgets import DirectoryTree, ClassicFooter, Static, TextArea, DataTable, Select, Button, Log, Input

from upath import UPath

from pyarrow.parquet import ParquetFile
from rich.text import Text, Style
import csv
from itertools import islice, chain
from datetime import datetime
import asyncio


class InputWithHistory(Input):
    BINDINGS = [
        ("up", "cmd_history(-1) "),
        ("down", "cmd_history(1) "),
        ('escape', 'defocus', 'Stop Typing')
    ]
    hsize = 20
    history = []
    step = None

    def action_defocus(self):
        self.blur()

    def action_cmd_history(self, direction: int):
        """
        """
        if not self.history:
            return

        if self.step is None and direction == -1:  # pressing up first time
            self.step = len(self.history) - 1

        elif self.step == len(self.history) - 1 and direction == 1:  # pressing down on most recent history item
            self.step = None

        elif self.step is not None:
            self.step = (self.step + direction) % len(self.history)

        self.clear()
        if self.step is not None:
            self.insert_text_at_cursor(self.history[self.step])

    def append_history(self, cmd: str):
        """
        """
        self.step = None
        while self.history and self.history[-1] == cmd:
            self.history.pop()

        self.history.append(cmd)
        self.history = self.history[-self.hsize:]


class UniversalDirectoryTree(DirectoryTree):
    PATH = UPath
    # tsstyle = Style(color='grey53')

    # def render_label(
    #     self, node, base_style, style
    # ):
    #     t = super().render_label(node, base_style, style)
    #     file_stat = node.data.path.stat()
    #     mtime = datetime.fromtimestamp(file_stat.st_mime).strftime('%V-%m-%a %H:%M')
    #     suff = Text.assemble(mtime)
    #
    #     suff.stylize_before(
    #         self.tsstyle
    #     )
    #     return Text.assemble(suff, t)


class CmdButton(Button):
    active_effect_duration = 0
    DEFAULT_CSS = """
    CmdButton {
        min-width: 0;
        height: 1;
        border: solid;
        border-top: none;
        border-bottom: none;
    
        &:hover {
            border-top: none;
        }
        &.-active {
            border-bottom: none;
            border-top: none;
        }
    }"""

    def __init__(self, label: str, cmd: str, number: int, **kwargs):
        self.cmd = cmd
        super().__init__(f'{label} ({number})', classes='cmd', id=f'cmd-{number}', **kwargs)


class BreadButton(Button):
    active_effect_duration = 0
    DEFAULT_CSS = """
    BreadButton {
        background: #2596be 50%;
        min-width:0;
        height: 1;
        border: none;
        border-top: none;
        border-bottom: none;
        
        &:hover {
            border-top: none;
        }
        &.-active {
            border-bottom: none;
            border-top: none;
        }
    }"""

    def __init__(self, path: UPath, **kwargs):
        self.path = path
        super().__init__(path.parts[-1], classes='bread', **kwargs)


class DirectoryTreeApp(App):
    # https://github.com/juftin/textual-universal-directorytree/blob/main/textual_universal directorytree/app.py
    BINDINGS = [
        ("escape", "quit", "Quit"),
        ('r', 'refresh', 'Refresh'),
        ('t', 'focus("cmd-input")', 'Terminal'),

        ('b', 'focus("browser")', 'Browser'),
        ('f', 'focus("file-content")', 'File'),
        ('d', 'focus("data-content")', "Table"),
        ('w', 'focus("log-output")', 'Log'),
        ('q', 'focus("drive-select")')
    ]
    CSS = """
    #metadata {
        height: 4;
        border: none
    }
    #drive-select {
        width: 30;
        background:  #4A148C;
        border: none;
        border-top: none;
        border-bottom: none;
        SelectOverlay {
            border: none;
            background: #4A148C;
        }
        SelectCurrent {
            border: none;
        }
    }
    #file-content {
        border: none
    }
    .single-line{
        height: 1;
        scrollbar-size-vertical: 0;
        scrollbar-size-horizontal: 0;
    }
    .bread-sep{
        width: auto;
        min-width: 0;
        height: 1;
        content-align: center middle;
    }
    .minor-title{
        background: blue 50%;
    }
    .small-scroll{
        scrollbar-size-vertical:1;
        scrollbar-size-horizontal:0;
    }
    #cmd-input {
        border: none;
        background: #4A148C;
    }
    #log-output {
        border: none;
    }
    #log-panel {
        height: 15;
    }
    """
    DRIVES = {
        # name: uri
        'root': '/',
        'home': '~/'
    }
    language_map = {
        '.yml': 'yaml',
        '.yaml': 'yaml',
        '.md': 'markdown',
        '.py': 'python',
        '.json': 'json',
        '.sh': 'bash'
    }

    cmd_map_local = {
        'du': 'du -s -h "{path}"',
        'mkdir': 'mkdir "{path}"',
        'cp': 'cp "{path}" ?',
        'open': 'open "{path}"',
        'mv':  'mv "{path}" ?',
    }

    def action_focus(self, dom_id):
        self.query_one(f"#{dom_id}").focus()

    def action_activate_cmd(self, number):
        self.query_one(f'#cmd-{number}', CmdButton).action_press()

    def action_refresh(self):

        if self.selected_node:
            if self.selected_node.data.path.exists():
                if self.selected_node.data.path.is_dir():
                    self.directory_tree.reload_node(self.selected_node)
            else:
                self.file_not_found()
                self.refresh_valid_parent(self.selected_node)

    def __init__(self, drive: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if drive not in self.DRIVES.items():
            self.drive: str = '~/'
        else:
            self.drive: str = drive

        init_root: UPath = UPath(self.drive).expanduser().resolve()
        self.selected_path: UPath = init_root
        self.selected_node = None

        # widgets / containers
        self.file_content = TextArea(id='file-content', classes='small-scroll', read_only=True, show_line_numbers=True)
        self.data_content = DataTable(id='data-content', classes='small-scroll')

        self.metadata = TextArea(id='metadata', classes='small-scroll', read_only=True, soft_wrap=False)
        self.address_bar = HorizontalScroll(
            *self.get_crumbs(self.selected_path),
            classes='single-line',
            id='address-bar'
        )

        self.directory_tree = UniversalDirectoryTree(id='browser', path=init_root, classes='small-scroll')
        self.cmd_input = InputWithHistory(id='cmd-input', classes='single-line')
        self.log_output = Log(id='log-output', classes=' small-scroll')

        self.data_first_n = 20
        self.data_col_first_n = 50
        self.drive_select_first_load = True

        self.cmd_timeout = 20

        for i, cmd in enumerate(self.cmd_map_local):
            self.bind(str(i + 1), f"activate_cmd({i + 1})")

    def get_crumbs(self, path: UPath):
        def _crumbs(path):
            while path != path.parent:
                if path.is_dir():
                    yield path
                path = path.parent
            yield path

        crumbs = list(_crumbs(path))
        crumbs_len = len(crumbs)
        crumbs = chain(
            *zip(
                (BreadButton(crumb) for crumb in reversed(crumbs)),
                (Static('›', classes='bread-sep') for _ in range(crumbs_len))
            )
        )
        return crumbs

    def populate_address_bar(self):
        self.address_bar.remove_children()
        if self.selected_path:
            crumbs = self.get_crumbs(self.selected_path)
            self.address_bar.mount(*crumbs)

    def compose(self) -> ComposeResult:
        yield Horizontal(
            Static(' (q)', classes='bread-sep'),

            Select(
                self.DRIVES.items(),
                id='drive-select',
                classes='single-line',
                allow_blank=False,
                value=self.drive
            ),
            self.address_bar,
            classes='single-line'
        )
        yield Horizontal(
            Vertical(
                Static(' (b) Browser - (r) Refresh selected folder', classes='minor-title'),
                self.directory_tree,
                Vertical(
                    Static(' (w) Logs', classes='minor-title'),
                    Horizontal(
                        *[CmdButton(cmd, cmd, i + 1) for i, cmd in enumerate(self.cmd_map_local)],
                        classes='single-line'
                    ),
                    self.log_output,
                    id='log-panel'
                )
            ),
            Vertical(
                Static('Metadata', classes='minor-title'),
                self.metadata,

                Static(' (f) File Preview (1,000 lines or 10,000 Bytes)', classes='minor-title'),
                self.file_content,

                Static(' (d) Data Preview (20 row X 50 col)', classes='minor-title'),
                self.data_content
            )
        )

        yield self.cmd_input
        yield ClassicFooter()

    def clear_previews(self):
        self.data_content.clear(columns=True)
        self.file_content.clear()
        self.file_content.language = None

    def file_not_found(self, m=''):
        self.notify(m, title='File/Folder not found', severity='warning')

    def refresh_valid_parent(self, node):
        # node.data. path
        # node. parent
        # reload node
        self.selected_node = None
        while node and not node.data.path.exists():
            node = node.parent

        if node:
            self.selected_path = node.data.path
            self.directory_tree.reload_node(node)

        else:  # reset tree
            root = UPath(self.drive).expanduser().resolve()
            self.selected_path = root
            self.clear_previews()
            self.directory_tree.path = root
            self.directory_tree.reload()

        self._update_meta(self.selected_path)
        self.populate_address_bar()

    @on(DirectoryTree.DirectorySelected)
    def handle_dir_selected(self, message: DirectoryTree.DirectorySelected) -> None:
        self.clear_previews()
        if message.path.exists():
            self.selected_node = message.node
            self.selected_path = message.path
            self._update_meta(self.selected_path)

            self.populate_address_bar()
        else:
            self.file_not_found()
            self.refresh_valid_parent(message.node)

    @on(BreadButton.Pressed, ".bread")
    def handle_bread_button_pressed(self, event: Button.Pressed):
        new_root = self._format_path(event.button.path)
        new_root = UPath(new_root)
        if not new_root.exists():
            self.file_not_found()

            while new_root != new_root.parent and not new_root.exists():
                new_root = new_root.parent

        self.directory_tree.path = new_root
        self.directory_tree.reload()
        self.selected_path = new_root
        self.selected_node = None
        self._update_meta(self.selected_path)
        self.populate_address_bar()

    def _format_path(self, path: UPath):
        render = str(path)
        return render

    def _update_meta(self, path: UPath):
        filestat = path.stat()
        render = self._format_path(path)
        updated = datetime.fromtimestamp(filestat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')

        if path.is_dir():
            metadata = f"""- Modified Time: {updated}
- Path: {render}"""

        else:
            metadata = f"""- Modified Time: {updated}
- Size: {filestat.st_size:,}
- Path: {render}"""

        self.metadata.text = metadata

    @on(DirectoryTree.FileSelected)
    def handle_file_selected(self, message: DirectoryTree.FileSelected) -> None:
        if not message.path.exists():
            self.file_not_found()
            self.refresh_valid_parent(message.node)
            return

        if self.selected_path == message.path:
            if self.selected_path.stat().st_mtime >= message.path.stat().st_mtime:
                return

        self.selected_node = message.node
        self.selected_path = message.path
        self._update_meta(self.selected_path)
        self.populate_address_bar()
        self.clear_previews()

        try:

            if message.path.suffix == '.csv':

                with message.path.open('r') as f:
                    rows = csv.reader(f)
                    header = list(next(rows))
                    sample_header = header[:self.data_col_first_n]

                    self.data_content.add_columns(*sample_header)
                    sample = (islice(r, len(sample_header)) for r in islice(rows, self.data_first_n))

                    self.data_content.add_rows(sample)
                    self.file_content.text = 'Columns:\n- ' + '\n- '.join(header)

            elif message.path.suffix == '. parquet':
                with message.path.open('rb') as f:
                    pf = ParquetFile(f)

                    first_n_rows = next(pf.iter_batches(batch_size=self.data_first_n))
                    header =[i.name for i in pf.schema_arrow]
                    sample_header = header[:self.data_col_first_n]
                    self.data_content.add_columns(*sample_header)

                    cols = [col for col in first_n_rows]
                    num_rows = len(cols[0])
                    if cols:
                        sample = (
                            (cols[j][i] for j in range( len(sample_header)) )
                            for i in range(num_rows)
                        )
                        self.data_content.add_rows(sample)
                        self.file_content.text = 'Columns:\n- ' + '\n- '.join(header)

            # 1000 lines
            elif message.path.suffix in self.language_map:
                lang = self.language_map.get(message.path.suffix)
                with message.path.open('r') as f:
                    self.file_content.language = lang
                    self.file_content.text = ''.join(islice(f, 1000))

            # 10000 bytes
            else:
                with message.path.open('rb') as f:
                    self.file_content.text = f.read(10000).decode()

        except Exception as e:
            self.log_output.write_line(str(e))

    @on(Select.Changed, '#drive-select')
    async def drive_select_changed(self, event: Select.Changed) -> None:
        if not self.drive_select_first_load:

            new_root = UPath(event.value).expanduser().resolve()
            self.selected_path = new_root
            self.selected_node = None
            self.drive = event.value

            self._update_meta(self.selected_path)
            self.clear_previews()
            self.directory_tree.path = new_root
            await self.directory_tree.reload()
            self.populate_address_bar()

        else:
            self.drive_select_first_load = False

        self.directory_tree.focus()

    @on(CmdButton.Pressed, '.cmd')
    def handle_cd_button_pressed(self, event: Button.Pressed):
        if not self.selected_path:
            return

        if self.selected_path.parent == self.selected_path:  # or self.selected _path == self.
            return

        if self.cmd_input.disabled:
            self.notify('Wait for last command to finish.')
            return

        path = self._format_path(self.selected_path)

        if event.button.cmd != 'path':
            self.cmd_input.clear()

        cmd = self.cmd_map_local[event.button.cmd].format(path=path)

        self.cmd_input.insert_text_at_cursor(cmd)
        self.cmd_input.focus()

    async def _submit_cmd(self, cmd: str):
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout_data, stderr_data = await asyncio.wait_for(proc.communicate(), self.cmd_timeout)
            self.log_output.write_line(f'< {proc.returncode}\n')
            self.log_output.write_lines (stdout_data.decode ().splitlines())

        except asyncio.TimeoutError:
            proc.kill()
            self.log_output.write_line(' < \n Timeout')

    def enable_input(self, task):
        self.cmd_input.disabled = False
        self.cmd_input.focus()

    @on(Input.Submitted, '#cmd-input')
    def submit_cmd(self, event: Input.Submitted) -> None:
        cmd = event.value.strip()

        if not cmd:
            return

        if cmd == 'exit':
            self.exit()

        self.cmd_input.append_history(cmd)
        self.log_output.write_line(' > \n' + cmd)
        self.cmd_input.clear()

        if event.value == 'clear':
            self.log_output.clear()
            return

        self.cmd_input.disabled = True

        asyncio.create_task(
            self._submit_cmd(cmd)
        ).add_done_callback(
            self.enable_input
        )


if __name__ == '__main__':

    app = DirectoryTreeApp()
    app.run()
