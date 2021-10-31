from aqt import mw
from PyQt5.QtGui import QKeySequence
from PyQt5.QtWidgets import QAction, QActionGroup, QMenu
from PyQt5 import QtCore, QtGui, QtWidgets
from anki.lang import _
from aqt import mw, utils
from aqt.qt import *
from .model import model
from .download import Ui_download
from .upload import Ui_upload
from .settings import Ui_settings
from os.path import dirname, exists, join, realpath
from json import dump, load

class Gui_Manager():
    def __init__(self):
        # -------------------------------
        # Boilerplate to hook up the GUI
        # -------------------------------
        # GUIs are tuples: [0] GUI setup class from QT creator, [1] Class to use
        self.dialog_gui_classes = {"Upload": (Ui_upload(), Upload_Dialog), "Download": (Ui_download(), Download_Dialog), "Settings": (Ui_settings(), Settings_Dialog)}
        self.dialogs = {}
        for k in self.dialog_gui_classes:
            cur_custom_class = self.dialog_gui_classes[k][1]
            cur_gui_obj = self.dialog_gui_classes[k][0]
            self.dialogs[k] = cur_custom_class()
            cur_gui_obj.setupUi(self.dialogs[k])
            self.dialogs[k].setup_gui(cur_gui_obj)
            self.dialogs[k].setup_actions(cur_gui_obj)
        # ------------------------------
        # Boilerplate ends here
        # ------------------------------

    def load_menu(self):
        for k in self.dialogs:
            add_menu_item("anki2notion",k,self.show_form_factory(self.dialogs[k], self.dialog_gui_classes[k][0]))
        if model.config["show_tests"]:
            add_menu_item("anki2notion::Tests","Get Databases",self.test_get_databases)
            add_menu_item("anki2notion::Tests","Get Records",self.test_get_records)

    def unload_menus(self):
        for menu in mw.custom_menus.values():
            mw.form.menubar.removeAction(menu.menuAction())
        mw.custom_menus.clear()

    def show_form_factory(self, dialog, form):
        return lambda: self.show_form_template(dialog, form)

    def show_form_template(self, dialog, form):
        dialog.setup_gui(form)
        dialog.setup_actions(form)
        dialog.exec()
    
    def test_get_databases(self):
        types = model.sync.get_anki_card_types()
        utils.showInfo( str(types[0]) )

    def test_get_records(self):
        saved_path = join(dirname(realpath(__file__)), 'anki_records.json')
        note_name = "Chinese (basic) course-2a807"
        deck_name = "Chinese"
        nt = mw.col.models.byName(note_name)
        cols = model.sync.anki_reader.get_columns(nt)
        records = model.sync.anki_reader.get_records(deck_name, note_name, cols)
        with open(saved_path, 'w', encoding='utf-8') as f:
            f.write(str(records))

class a2n_Dialog(QDialog):
    def __init__(self, parent=None):
        self.parent = parent
        self.actions_setup = False
        QDialog.__init__(self, parent, Qt.Window)

    def setup_actions(self, form=None):
        if not self.actions_setup: # we don't want to set up gui actions over and over again
            self.actions_setup = True
            self._setup_actions(form)

    def _setup_actions(self, form=None):
        QtCore.QMetaObject.connectSlotsByName(self)

    def setup_gui(self, form):
        pass

class Upload_Dialog(a2n_Dialog):
    def _setup_actions(self, form):
        form.cancel_button.clicked.connect(lambda: close_form(self))
        super().setup_actions(form)

    def setup_gui(self, form):
        form.sync_mode.setCurrentIndex( model.get_merge_mode() )

class Download_Dialog(a2n_Dialog):
    def _setup_actions(self, form):
        form.cancel_button.clicked.connect(lambda: close_form(self))
        super().setup_actions(form)

    def setup_gui(self, form):
        form.sync_mode.setCurrentIndex( model.get_merge_mode() )

class Settings_Dialog(a2n_Dialog):
    def _setup_actions(self, form):
        form.cancel_button.clicked.connect(lambda: close_form(self))
        form.save_button.clicked.connect(lambda: self.save_key(form))
        super().setup_actions(form)

    def setup_gui(self, form):
        form.api_key.setText( model.get_notion_key() )
        form.merge_mode.setCurrentIndex ( model.get_merge_mode() ) # uses same values as sync/MERGE_TYPE

    def save_key(self, form):
        model.save_merge_mode(form.merge_mode.currentIndex())
        model.save_notion_key(form.api_key.text())
        self.close()

def close_form(form):
    form.close()

def add_menu(path):
    if not hasattr(mw, 'custom_menus'):
        mw.custom_menus = {}

    if len(path.split('::')) == 2:
        parent_path, child_path = path.split('::')
        has_child = True
    else:
        parent_path = path
        has_child = False

    if parent_path not in mw.custom_menus:
        parent = QMenu('&' + parent_path, mw)
        mw.custom_menus[parent_path] = parent
        mw.form.menubar.insertMenu(mw.form.menuTools.menuAction(), parent)

    if has_child and (path not in mw.custom_menus):
        child = QMenu('&' + child_path, mw)
        mw.custom_menus[path] = child
        mw.custom_menus[parent_path].addMenu(child)


def add_menu_item(path, text, func, keys=None, checkable=False, checked=False):
    action = QAction(text, mw)

    if keys:
        action.setShortcut(QKeySequence(keys))

    if checkable:
        action.setCheckable(checkable)
        action.toggled.connect(func)
        if not hasattr(mw, 'action_groups'):
            mw.action_groups = {}
        if path not in mw.action_groups:
            mw.action_groups[path] = QActionGroup(None)
        mw.action_groups[path].addAction(action)
        action.setChecked(checked)
    else:
        action.triggered.connect(func)

    if path == 'File':
        mw.form.menuCol.addAction(action)
    elif path == 'Edit':
        mw.form.menuEdit.addAction(action)
    elif path == 'Tools':
        mw.form.menuTools.addAction(action)
    elif path == 'Help':
        mw.form.menuHelp.addAction(action)
    else:
        add_menu(path)
        mw.custom_menus[path].addAction(action)

# ---------------------------
gui = Gui_Manager()