from anki import hooks
from aqt import gui_hooks
from aqt import mw
from anki.cards import Card
from aqt.qt import *
from aqt.utils import showInfo, qconnect

from PyQt5 import QtCore, QtGui, QtWidgets

def add_action_to_menu(action, target_function):
    qconnect(action.triggered, target_function)
    mw.form.menuTools.addAction(action)

def notion_upload():
    pass

def notion_download():
    pass

class Upload_Dialog(QDialog):
    def __init__(self, parent=None):
        self.parent = parent
        QDialog.__init__(self, parent, Qt.Window)
    


class Upload_Ui(object):
    def __init__(self):
        self.dialog = Upload_Dialog()

    def setup_dialog(self, dialog):
        dialog.setObjectName("dialog")
        dialog.resize(279, 75)
        self.widget = QWidget(dialog)
        self.widget.setGeometry(QRect(0, 10, 271, 53))
        self.widget.setObjectName("widget")
        self.verticalLayout = QVBoxLayout(self.widget)
        self.verticalLayout.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout.setObjectName("verticalLayout")
        QMetaObject.connectSlotsByName(dialog)
        dialog.exec()

    def show_ui(self):
        self.setup_dialog(self.dialog)

class Download_Box:
    def __init__(self, parent=None):
        self.parent = parent
        QDialog.__init__(self, parent, Qt.Window)


    def show_ui(self):
        pass


def create_upload_box():
    up = Upload_Ui()
    up.show_ui()

upload_action = QAction("Upload to Notion", mw)
# download_action = QAction("Download from Notion", mw)
add_action_to_menu(upload_action, create_upload_box)
# add_action_to_menu(download_action, notion_download)