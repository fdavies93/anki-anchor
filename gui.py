from aqt import mw
from PyQt5.QtGui import QKeySequence
from PyQt5.QtWidgets import QAction, QActionGroup, QMenu
from anki.lang import _
from aqt import mw
from aqt.qt import *
from .model import model

class Gui_Manager():
    def __init__(self):
        self.dialogs = {"Upload": Upload_Ui(), "Download": Download_Ui(), "Settings": Settings_Ui()}

    def load_menu(self):
        for k in self.dialogs:
            add_menu_item("anki2notion",k,self.dialogs[k].show)

    def unload_menus(self):
        for menu in mw.custom_menus.values():
            mw.form.menubar.removeAction(menu.menuAction())
        mw.custom_menus.clear()

class a2n_Dialog(QDialog):
    def __init__(self, parent=None):
        self.parent = parent
        QDialog.__init__(self, parent, Qt.Window)

# This class is designed to be overridden; it just sets up a basic object structure for orchestrating dialogs
class a2n_Ui(object):
    def __init__(self):
        self.dialog = a2n_Dialog()
    
    def setup_dialog(self, dialog):
        dialog.setObjectName("dialog")
        dialog.resize(640, 480)
        self.widget = QWidget(dialog)
        self.widget.setGeometry(QRect(0, 10, 640, 480))
        self.widget.setObjectName("widget")
        QMetaObject.connectSlotsByName(dialog)

    def show(self):
        self.setup_dialog(self.dialog)
        self.dialog.exec()

class Upload_Ui(a2n_Ui):
    def setup_dialog(self, dialog):
        pass

class Download_Ui(a2n_Ui):
    def setup_dialog(self, dialog):
        pass

class Settings_Ui(a2n_Ui):
    def setup_dialog(self, dialog):
        dialog.setObjectName("dialog")
        dialog.resize(640, 480)
        dialog.widget = QWidget(dialog)
        dialog.widget.setGeometry(QRect(0, 10, 640, 480))
        dialog.widget.setObjectName("widget")
        dialog.qvbox = QVBoxLayout(dialog.widget)
        dialog.qvbox.setObjectName("qvbox")
        dialog.api_key_text = QLineEdit(model.config["notion_key"],dialog.widget)
        dialog.api_key_text.setObjectName("api_key_text")
        dialog.savebutton = QPushButton("Save",dialog.widget)
        dialog.savebutton.setObjectName("savebutton")
        dialog.cancelbutton = QPushButton("Cancel",dialog.widget)
        dialog.cancelbutton.setObjectName("cancelbutton")

        for o in (dialog.api_key_text, dialog.savebutton, dialog.cancelbutton):
            dialog.qvbox.addWidget(o)

        QMetaObject.connectSlotsByName(dialog)

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