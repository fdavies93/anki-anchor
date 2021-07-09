from aqt import mw
from PyQt5.QtGui import QKeySequence
from PyQt5.QtWidgets import QAction, QActionGroup, QMenu
from anki.lang import _
from aqt import mw
from aqt.qt import *
from .model import model
from .download import Download_Ui
from .upload import Upload_Ui
from .settings import Settings_Ui

class Gui_Manager():
    def __init__(self):
        self.dialog_classes = {"Upload": Upload_Ui(), "Download": Download_Ui(), "Settings": Settings_Ui()}
        self.dialogs = {}
        for k in self.dialog_classes:
            self.dialogs[k] = a2n_Dialog()
            self.dialog_classes[k].setupUi(self.dialogs[k])

    def load_menu(self):
        for k in self.dialogs:
            add_menu_item("anki2notion",k,self.show_form_factory(self.dialogs[k]))

    def unload_menus(self):
        for menu in mw.custom_menus.values():
            mw.form.menubar.removeAction(menu.menuAction())
        mw.custom_menus.clear()

    def show_form_factory(self, form):
        return lambda _: form.exec()

class a2n_Dialog(QDialog):
     def __init__(self, parent=None):
        self.parent = parent
        QDialog.__init__(self, parent, Qt.Window)

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