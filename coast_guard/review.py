#!/usr/bin/env python

"""
An interactive graphical user interface to review failed
data reduction.

Patrick Lazarus, Jan 17, 2014
"""

import sys
import datetime

from PyQt4 import QtGui as qtgui
from PyQt4 import QtCore as qtcore
import ui_reviewer

import database
import utils
import reduce_data


class FailedFilesModel(qtcore.QAbstractTableModel):
    def __init__(self, priorities=None, parent=None):
        super(FailedFilesModel, self).__init__(parent)

        self.priorities = priorities
        # Establish a database object
        self.db = database.Database()
        # Fetch information about files that 
        # failed processing from database
        self.__files = self.fetch_data_from_database()
        self.__headers = []
        # Get header names from database column names
        for hdr in self.__files[0].keys():
            self.__headers.append(hdr)
        self.__reattempted = []

    def headerData(self, section, orientation, role=qtcore.Qt.DisplayRole):
        if role == qtcore.Qt.DisplayRole:
            if orientation == qtcore.Qt.Horizontal:
                return self.__headers[section]

    def rowCount(self, parent=None):
        return len(self.__files)

    def columnCount(self, parent=None):
        return len(self.__files[0]) 

    def flags(self, index):
        return qtcore.Qt.ItemIsEnabled | qtcore.Qt.ItemIsSelectable

    def data(self, index, role=qtcore.Qt.DisplayRole):
        val = None
        row = index.row()
        col = index.column()
        if role in (qtcore.Qt.DisplayRole, qtcore.Qt.EditRole):
            val = self.__files[row][col]
            if col == 12:
                with open(val, 'r') as logfile:
                    val = logfile.read()
        elif role == qtcore.Qt.BackgroundRole:
            if row in self.__reattempted:
                val = qtgui.QBrush(qtgui.QColor('#90EE90'))
        return val

    def reattempt_file(self, index):
        row = index.row()
        fileinfo = self.__files[row]
        file_id = fileinfo['file_id']
        if fileinfo['stage'] == 'calibrated' and fileinfo['qcpassed']:
            values = {'status': 'toload'}
        else:
            values = {'status': 'new'}
        with self.db.transaction() as conn:
            update = self.db.files.update().\
                        where(self.db.files.c.file_id == file_id).\
                        values(last_modified=datetime.datetime.now())
            conn.execute(update, values)
        self.__reattempted.append(row)

    def fetch_data_from_database(self):
        whereclause = (self.db.files.c.status.in_(['submitted',
                                                   'running',
                                                   'failed', 
                                                   'calfail']))& \
                      ((self.db.files.c.qcpassed.is_(None)) | \
                       (self.db.files.c.qcpassed == True))
        if self.priorities:
            priority_list = []
            for pr in self.priorities:
                priority_list.extend(reduce_data.parse_priorities(pr))
            prioritizer, cfgstr = priority_list[0]
            tmp = prioritizer(self.db, cfgstr)
            for prioritizer, cfgstr in priority_list[1:]:
                tmp |= prioritizer(self.db, cfgstr)
            whereclause &= tmp
        with self.db.transaction() as conn:
            select = self.db.select([self.db.files.c.file_id,
                                     self.db.files.c.obs_id,
                                     self.db.obs.c.sourcename,
                                     self.db.obs.c.rcvr,
                                     self.db.files.c.stage,
                                     self.db.obs.c.start_mjd,
                                     self.db.obs.c.obstype,
                                     self.db.files.c.status,
                                     self.db.files.c.qcpassed,
                                     self.db.files.c.filepath,
                                     self.db.files.c.filepath + '/' +self.db.files.c.filename,
                                     self.db.files.c.note,
                                     self.db.logs.c.logpath + '/' +
                                     self.db.logs.c.logname],
                        from_obj=[self.db.obs.
                            outerjoin(self.db.files,
                                onclause=self.db.files.c.file_id ==
                                        self.db.obs.c.current_file_id).
                            outerjoin(self.db.logs,
                                onclause=self.db.obs.c.obs_id ==
                                        self.db.logs.c.obs_id)]).\
                        where(whereclause)
            result = conn.execute(select)
            rows = result.fetchall()
            result.close()
        return rows


class Reviewer(qtgui.QWidget, ui_reviewer.Ui_Reviewer):
    """Failed-job reviewer window.
    """
    def __init__(self, priorities=None):
        super(Reviewer, self).__init__()
        # Set up the window
        self.setupUi(self)
        self.__set_keyboard_shortcuts()
        self.__connect_signals()

        # Set up model
        self.__model = FailedFilesModel(priorities=priorities)
        
        # Initialize
        self.tableview.setModel(self.__model)
        self.tableview.setUpdatesEnabled(True)
        for icol in range(9, self.__model.columnCount()):
            self.tableview.setColumnHidden(icol, True)

        # Set up data-widget mapping
        self.mapper = qtgui.QDataWidgetMapper()
        self.mapper.setModel(self.__model)
        self.mapper.addMapping(self.psr_text, 2)
        self.mapper.addMapping(self.rcvr_text, 3)
        self.mapper.addMapping(self.stage_text, 4)
        self.mapper.addMapping(self.mjd_text, 5)
        self.mapper.addMapping(self.obstype_text, 6)
        self.mapper.addMapping(self.file_text, 10)
        self.mapper.addMapping(self.notes_text, 11)
        self.mapper.addMapping(self.log_text, 12)

        # Link tableview's selection to the data-widget mapper
        self.selection = self.tableview.selectionModel()
        self.selection.currentRowChanged.connect(self.select_row)

    def __set_keyboard_shortcuts(self):
        qtgui.QShortcut(qtcore.Qt.Key_Q, self, self.close)
        qtgui.QShortcut(qtcore.Qt.Key_R, self, self.reattempt_selected)
        qtgui.QShortcut(qtcore.Qt.Key_L, self, lambda: self.tab.setCurrentWidget(self.log_tab))
        qtgui.QShortcut(qtcore.Qt.Key_N, self, lambda: self.tab.setCurrentWidget(self.notes_tab))

    def __connect_signals(self):
        self.reatt_button.clicked.connect(self.reattempt_selected)

    def reattempt_selected(self):
        index = self.selection.currentIndex()
        self.__model.reattempt_file(index)
        self.tableview.clearFocus()
        self.tableview.setFocus()
    
    def select_row(self, newindex, oldindex):
        self.mapper.setCurrentModelIndex(newindex)


def main():
    app = qtgui.QApplication(sys.argv)
    
    review_win = Reviewer(priorities=args.priority)
    # Display the window
    review_win.show()

    exitcode = app.exec_()
    sys.exit(exitcode)


if __name__ == "__main__":
    parser = utils.DefaultArguments(description="Review files that failed "
                                    "automated Asterix data reduction jobs.")
    parser.add_argument("--prioritize", action='append',
                        default=[], dest='priority',
                        help="A rule for prioritizing observations.")
    args = parser.parse_args()
    main()
