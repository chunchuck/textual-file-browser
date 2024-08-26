from textual import on
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical, HorizontalScroll
from textual.widgets import DirectoryTree, Static, TextArea, DataTable, Select, Button, Log, Input, Markdown

from upath import UPath
from pyarrow.parquet import ParquetFile
from rich.text import Text, Style
import csv
from itertools import islice, chain
from datetime import datetime
import asyncio
import json


def ipynb_lines(path: UPath):
    with path.open() as f:
        j = json.load(f)

        for cell in j['cells']:

            if cell['cell_type'] in {'code', 'markdown'}:
                yield '[ ]:\n'
                yield from cell['source']
                yield '\n'


class InputWithHistory(Input):
    BINDINGS = [
        ("up", "cmd_history(-1)"),
        ("down", "cmd_history(1)"),
        ('escape', 'defocus')
    ]
    DEFAULT_CSS = """
    Input>.input--placeholder, Input>.input--suggestion {
        color: #d6d6d6;
    }"""

    history_size = 20
    history = []
    step = None

    def action_defocus(self):
        self.app.directory_tree.focus()

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
        self.history = self.history[-self.history_size:]


class InputSearch(Input):
    BINDINGS = [
        ('escape', 'stop_search'),
        ('up', 'search(-1)'),
        ('down', 'search(1)')
    ]
    DEFAULT_CSS = """
    Input>.input--placeholder, Input>.input--suggestion {
        color: #d6d6d6;
    }"""

    def action_stop_search(self):
        self.app.directory_tree.file_filter = None
        self.app.directory_tree.focus()
        # self.app.directory_tree.refresh_children()
        self.app.directory_tree.refresh_searched()
        self.clear()

    def action_search(self, direction):
        self.app.search_files_and_scroll(self.value, scroll_dir=direction)


class UniversalDirectoryTree(DirectoryTree):
    PATH = UPath
    BINDINGS = [('enter', 'cd'), ('space', 'cd')]

    not_found = Style(color='grey53')
    file_filter = None
    found_node_idx: list = None
    found_node_cursor = 0

    async def action_cd(self):
        node = self.get_node_at_line(
            self.cursor_line
        )
        if node.data.path.is_dir():
            await self.app.cd(node.data.path)
        else:
            self.select_node(node)

    def render_label(
        self, node, base_style, style
    ):
        t = super().render_label(node, base_style, style)
        if self.file_filter and self.file_filter not in node.data.path.parts[-1]:
            return Text.assemble(t, style=self.not_found)
        else:
            return t

    def refresh_children(self):
        for line in self._tree_lines:
            line.node.refresh()

    def refresh_searched(self):
        self.found_node_idx = []
        if self.file_filter:
            self.found_node_idx = [
                i
                for i, line in enumerate(self._tree_lines)
                if self.file_filter in line.node.data.path.parts[-1]:
            ]

        if self.found_node_cursor > len(self.found_node_idx):
            self.found_node_cursor = 0

    def scroll_next(self, steps: int):
        if self.found_node_idx:
            self.found_node_cursor = (
                self.found_node_cursor + steps
            ) % len(self.found_node_idx)

            node = self._tree_lines[self.found_node_idx[
                self.found_node_cursor
            ]].node

            self.scroll_to_node(node, animate=False)
            self.move_cursor(node)


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

        ('r', 'refresh', 'Refresh'),
        (':', 'focus("cmd-input")'),
        ('/', 'focus("search")'),

        ('escape', 'cd_parent'),

        ('b', 'focus("browser")'),
        ('f', 'focus("file-content")'),
        ('d', 'focus("data-content")'),
        ('w', 'focus("log-output")'),
        ('q', 'show_overlay'),
        ('f1', 'paste_path')

    ]
    CSS = """
    #metadata {
        height: 4;
        border: none
    }
    #search {
        border: none;
        background: blue 50%;
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
        'root': 'file:///'
    }
    language_map = {
        '.yml': 'yaml',
        '.yaml': 'yaml',
        '.md': 'markdown',
        '.py': 'python',
        '.json': 'json',
        '.sh': 'bash',
        '.sql': 'sql'
    }

    cmd_map_local = {
        'du': 'du -s -h "{path}"',
        'mkdir': 'mkdir "{path}"',
        'cp': 'cp "{path}" ?',
        'open': 'open "{path}"',
        'mv':  'mv "{path}" ?',
    }

    # Actions =======================================================
    def action_show_overlay(self):
        self.query_one('#drive-select').action_show_overlay()

    async def action_cd_parent(self):
        if self.directory_tree.root.data.path.parent != self.directory_tree.root.data.path:
            await self.cd(self.directory_tree.root.data.path.parent)

    def action_paste_path(self):
        node = self.directory_tree.get_node_at_line(
            self.directory_tree.cursor_line
        )
        if not self.cmd_input.disabled:
            self.cmd_input.insert_text_at_cursor('"' + str(node.data.path) + '"')
            self.cmd_input.focus()

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
    # Actions =======================================================

    def __init__(self, drive: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if drive not in self.DRIVES.items():
            self.drive: str = 'file:///'
        else:
            self.drive: str = drive

        init_root: UPath = UPath(self.drive).resolve()
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
        self.cmd_input = InputWithHistory(
            id='cmd-input', classes='single-line', placeholder='(:) Command (up) (down) (esc) x'
        )
        self.search = InputSearch(id='search', classes='single-line',
                                  placeholder='(/) Search (up) (down) (esc) x')
        self.log_output = Log(id='log-output', classes=' small-scroll')

        # misc
        self.data_first_n = 20
        self.data_col_first_n = 50
        self.drive_select_first_load = True

        self.cmd_timeout = 20
        self.just_changed_dir = True

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
            Static(' <|esc c|>', classes='bread-sep'),
            self.address_bar,
            classes='single-line'
        )
        yield Horizontal(
            Vertical(
                self.search,
                self.directory_tree,
                Vertical(
                    Static(' (w) Logs v (b) Browder ^', classes='minor-title'),
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

    def clear_previews(self):
        self.data_content.clear(columns=True)
        self.file_content.clear()
        self.file_content.language = None

    def file_not_found(self, m=''):
        self.notify(m, title='File/Folder not found', severity='warning')

    async def refresh_valid_parent(self, node):
        # node.data. path
        # node. parent
        # reload node
        self.selected_node = None
        while node and not node.data.path.exists():
            node = node.parent

        if node:
            p = self.directory_tree.reload_node(node)
            self.selected_path = node.data.path
            self._update_meta(self.selected_path)
            self.populate_address_bar()
            await p
        else:
            root = UPath(self.drive).resolve()
            await self.cd(root)

    @on(DirectoryTree.DirectorySelected)
    async def handle_dir_selected(self, message: DirectoryTree.DirectorySelected) -> None:
        self.clear_previews()
        if message.path.exists():
            self.selected_node = message.node
            self.selected_path = message.path
            self._update_meta(self.selected_path)
            self.populate_address_bar()

        else:
            p = self.refresh_valid_parent(message.node)
            self.file_not_found()
            await p

        self.just_changed_dir = True

    @on(BreadButton.Pressed, ".bread")
    async def handle_bread_button_pressed(self, event: Button.Pressed):
        await self.cd(event.button.path)
        self.directory_tree.focus()

    def _format_path(self, path: UPath):
        render = str(path)
        if path.protocol == 'file':
            render = render.replace(f'file://', '')
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
    async def handle_file_selected(self, message: DirectoryTree.FileSelected) -> None:
        if not message.path.exists():
            self.file_not_found()
            await self.refresh_valid_parent(message.node)
            return

        self.selected_node = message.node
        self.selected_path = message.path
        self._update_meta(self.selected_path)
        self.populate_address_bar()
        self.clear_previews()

        try:
            if message.path.suffix == '.ipynb':
                self.file_content.load_text(''.join(islice(
                    ipynb_lines(message.path),
                    1000
                )))

            elif message.path.suffix == '.csv':

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

    async def cd(self, path: UPath):
        if not path.is_dir():
            path = path.parent

        if not path.exists():
            self.file_not_found()
            while path != path.parent and not path.exists():
                path = path.parent

        self.directory_tree.path = path
        p = self.directory_tree.reload()

        self.selected_path = path
        self.selected_node = None

        self._update_meta(self.selected_path)
        self.clear_previews()
        self.populate_address_bar()

        self.just_changed_dir = True

        await p

    @on(Select.Changed, '#drive-select')
    async def drive_select_changed(self, event: Select.Changed) -> None:

        if not self.drive_select_first_load:

            new_root = UPath(event.value).resolve()
            self.drive = event.value
            await self.cd(new_root)

        else:
            self.drive_select_first_load = False

        self.directory_tree.focus()

    @on(CmdButton.Pressed, '.cmd')
    def handle_cmd_button_pressed(self, event: Button.Pressed):

        node = self.directory_tree.get_node_at_line(
            self.directory_tree.cursor_line
        )

        if self.cmd_input.disabled:
            self.notify('Wait for last command to finish.')
            return

        path = self._format_path(node.data.path)

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
            self.log_output.write_lines(stdout_data.decode ().splitlines())

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

    @on(Input.Submitted, '#search')
    async def search(self, event: Input.Submitted) -> None:
        if event.value and self.directory_tree.file_filter == event.value:
            node = self.directory_tree.get_node_at_line(
                self.directory_tree.cursor_line
            )
            if node.data.path.is_dir():
                await self.cd(node.data.path)
            else:
                self.directory_tree.select_node(node)

        else:
            self.search_files_and_scroll(event.value)

    # @on(DirectoryTree.NodeCollapsed)
    # def togglec(self, event: DirectoryTree.NodeCollapsed):
    #     self.notify('c')
    #     self.just_changed_dir = True
    #
    # @on(DirectoryTree.NodeExpanded)
    # def togglee(self, event: DirectoryTree.NodeExpanded):
    #     self.notify('e')
    #     self.just_changed_dir = True

    @on(Input.Changed, '#search')
    def search_on_type(self, event: Input.Submitted) -> None:
        if self.search.has_focus:
            self.directory_tree.file_filter = event.value
            self.directory_tree.refresh_children()
            self.directory_tree.refresh_searched()
            self.directory_tree.scroll_next(0)

    def search_files_and_scroll(self, search_term: str, scroll_dir=1):
        if search_term:
            if self.directory_tree.file_filter != search_term:
                self.directory_tree.file_filter = search_term
                self.directory_tree.refresh_children()
                self.directory_tree.refresh_searched()
                self.directory_tree.scroll_next(0)

                # num_found = len(self.directory_tree.found_node_idx)
                # self.log_output.write_line(f'Search {search_term} found {num_found}')

            else:
                if self.just_changed_dir:
                    self.directory_tree.refresh_searched()
                    self.just_changed_dir = False

                self.directory_tree.scroll_next(scroll_dir)

        else:
            self.directory_tree.file_filter = None
            self.directory_tree.refresh_searched()
            self.directory_tree.focus()


if __name__ == '__main__':

    app = DirectoryTreeApp()
    app.run()
