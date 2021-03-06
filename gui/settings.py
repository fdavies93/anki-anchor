# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file './qt_ui/settings.ui'
#
# Created by: PyQt5 UI code generator 5.15.4
#
# WARNING: Any manual changes made to this file will be lost when pyuic5 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_settings(object):
    def setupUi(self, settings):
        settings.setObjectName("settings")
        settings.resize(640, 163)
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(10)
        settings.setFont(font)
        self.horizontalLayoutWidget = QtWidgets.QWidget(settings)
        self.horizontalLayoutWidget.setGeometry(QtCore.QRect(20, 90, 601, 61))
        self.horizontalLayoutWidget.setObjectName("horizontalLayoutWidget")
        self.horizontalLayout = QtWidgets.QHBoxLayout(self.horizontalLayoutWidget)
        self.horizontalLayout.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.save_button = QtWidgets.QPushButton(self.horizontalLayoutWidget)
        self.save_button.setLocale(QtCore.QLocale(QtCore.QLocale.English, QtCore.QLocale.UnitedStates))
        self.save_button.setObjectName("save_button")
        self.horizontalLayout.addWidget(self.save_button)
        self.cancel_button = QtWidgets.QPushButton(self.horizontalLayoutWidget)
        self.cancel_button.setLocale(QtCore.QLocale(QtCore.QLocale.English, QtCore.QLocale.UnitedStates))
        self.cancel_button.setObjectName("cancel_button")
        self.horizontalLayout.addWidget(self.cancel_button)
        self.formLayoutWidget = QtWidgets.QWidget(settings)
        self.formLayoutWidget.setGeometry(QtCore.QRect(20, 20, 601, 59))
        self.formLayoutWidget.setObjectName("formLayoutWidget")
        self.formLayout = QtWidgets.QFormLayout(self.formLayoutWidget)
        self.formLayout.setContentsMargins(0, 0, 0, 0)
        self.formLayout.setObjectName("formLayout")
        self.label = QtWidgets.QLabel(self.formLayoutWidget)
        self.label.setAcceptDrops(False)
        self.label.setScaledContents(False)
        self.label.setObjectName("label")
        self.formLayout.setWidget(0, QtWidgets.QFormLayout.LabelRole, self.label)
        self.api_key = QtWidgets.QLineEdit(self.formLayoutWidget)
        self.api_key.setObjectName("api_key")
        self.formLayout.setWidget(0, QtWidgets.QFormLayout.FieldRole, self.api_key)
        self.label_3 = QtWidgets.QLabel(self.formLayoutWidget)
        self.label_3.setObjectName("label_3")
        self.formLayout.setWidget(1, QtWidgets.QFormLayout.LabelRole, self.label_3)
        self.merge_mode = QtWidgets.QComboBox(self.formLayoutWidget)
        self.merge_mode.setObjectName("merge_mode")
        self.merge_mode.addItem("")
        self.merge_mode.addItem("")
        self.merge_mode.addItem("")
        self.formLayout.setWidget(1, QtWidgets.QFormLayout.FieldRole, self.merge_mode)

        self.retranslateUi(settings)
        QtCore.QMetaObject.connectSlotsByName(settings)

    def retranslateUi(self, settings):
        _translate = QtCore.QCoreApplication.translate
        settings.setWindowTitle(_translate("settings", "Settings"))
        self.save_button.setText(_translate("settings", "Save"))
        self.cancel_button.setText(_translate("settings", "Cancel"))
        self.label.setText(_translate("settings", "Notion API Key"))
        self.label_3.setText(_translate("settings", "Default Merge Mode"))
        self.merge_mode.setItemText(0, _translate("settings", "Append"))
        self.merge_mode.setItemText(1, _translate("settings", "Soft Merge"))
        self.merge_mode.setItemText(2, _translate("settings", "Hard Merge"))
