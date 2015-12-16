#!/usr/bin/env python

"""
An interactive graphical user interface to check the quality
of automatically reduced data files.

Patrick Lazarus, Nov 12, 2013
"""

import sys
import os
import os.path
import datetime
import tempfile
import shutil
import warnings

from PyQt4 import QtGui as qtgui
from PyQt4 import QtCore as qtcore

from coast_guard import config
from coast_guard import database
from coast_guard import utils
from coast_guard import errors
from coast_guard import reduce_data
from coast_guard import add_missing_summary_plots as amsp

class QualityControl(qtgui.QWidget):
    """Quality control window.
    """

    def __init__(self, priorities=None, stage='cleaned', re_eval=False):
        super(QualityControl, self).__init__()
        # Set up the window
        self.__setup()
        self.__add_widgets()
        self.__set_keyboard_shortcuts()

        # Establish a database object
        self.db = database.Database()
        
        # Initialize
        self.priorities = priorities
        self.stage = stage
        self.re_eval = re_eval
        self.idiag = 0
        self.file_id = None
        self.diagplots = []

    def __setup(self):
        # Geometry arguments: x, y, width, height (all in px)
        #self.setGeometry(0, 0, 900, 700)
        self.setWindowTitle("Coast Guard Quality Control")

    def __set_keyboard_shortcuts(self):
        qtgui.QShortcut(qtcore.Qt.Key_Space, self, self.cycle_diag_fwd)
        qtgui.QShortcut(qtgui.QKeySequence(qtcore.Qt.SHIFT+qtcore.Qt.Key_Space), \
                        self, self.cycle_diag_rev)
        qtgui.QShortcut(qtcore.Qt.Key_G, self, self.set_file_as_good)
        qtgui.QShortcut(qtcore.Qt.Key_W, self, 
                        lambda: self.set_file_as_good(note='weak'))
        qtgui.QShortcut(qtcore.Qt.Key_B, self, self.set_file_as_bad)
        qtgui.QShortcut(qtcore.Qt.Key_E, self, 
                        lambda: self.set_file_as_good(note='ephem'))
        qtgui.QShortcut(qtcore.Qt.Key_N, self,
                        lambda: self.set_file_as_bad(reason='nondetect'))
        qtgui.QShortcut(qtcore.Qt.Key_Z, self, self.zap_file_manually)
        qtgui.QShortcut(qtcore.Qt.SHIFT+qtcore.Qt.Key_Z,
                        self, lambda: self.zap_file_manually(reset_weights=True))
        qtgui.QShortcut(qtcore.Qt.Key_S, self, self.advance_file)
        qtgui.QShortcut(qtcore.Qt.Key_R, self, self.get_files_to_check)
        qtgui.QShortcut(qtcore.Qt.Key_Q, self, self.close)
        qtgui.QShortcut(qtcore.Qt.Key_P, self, self.set_priorities)
        qtgui.QShortcut(qtcore.Qt.Key_A, self, self.add_parents_diags)
        qtgui.QShortcut(qtcore.Qt.Key_F, self, self.write_filename)
        qtgui.QShortcut(qtcore.Qt.Key_C, self, self.add_cal_diags)

    def __add_widgets(self):
        self.image_holder = qtgui.QLabel()
        self.plot_lbl = qtgui.QLabel()
        
        prev_button = qtgui.QPushButton("Prev. Diagnostic Plot")
        prev_button.clicked.connect(self.cycle_diag_rev)
        next_button = qtgui.QPushButton("Next Diagnostic Plot")
        next_button.clicked.connect(self.cycle_diag_fwd)

        good_button = qtgui.QPushButton("&Good")
        good_button.clicked.connect(self.set_file_as_good)
        weak_button = qtgui.QPushButton("&Weak")
        weak_button.clicked.connect(lambda: self.set_file_as_good(note='weak'))
        bad_button = qtgui.QPushButton("&Bad")
        bad_button.clicked.connect(self.set_file_as_bad)
        ephem_button = qtgui.QPushButton("Bad &Ephem")
        ephem_button.clicked.connect(
            lambda: self.set_file_as_good(note='ephem'))
        nodetect_button = qtgui.QPushButton("&No Detect")
        nodetect_button.clicked.connect(
            lambda: self.set_file_as_bad(reason='nondetect'))
        zap_button = qtgui.QPushButton("&Zap")
        zap_button.clicked.connect(self.zap_file_manually)
        skip_button = qtgui.QPushButton("&Skip")
        skip_button.clicked.connect(self.advance_file)
        reload_button = qtgui.QPushButton("&Reload")
        reload_button.clicked.connect(self.get_files_to_check)
        priority_button = qtgui.QPushButton("&Prioritize")
        priority_button.clicked.connect(self.set_priorities)
        addparents_button = qtgui.QPushButton("&Add plots")
        addparents_button.clicked.connect(self.add_parents_diags)

        # Counter for the number of plots left
        self.lcd = qtgui.QLCDNumber(4)
        self.lcd.setSegmentStyle(2)  # Flat style

        plotctrl_box = qtgui.QHBoxLayout()
        plotctrl_box.addWidget(prev_button)
        plotctrl_box.addStretch(1)
        plotctrl_box.addWidget(self.plot_lbl)
        plotctrl_box.addStretch(1)
        plotctrl_box.addWidget(next_button)

        left_box = qtgui.QVBoxLayout()
        left_box.addWidget(self.image_holder)
        left_box.addStretch(1)
        left_box.addLayout(plotctrl_box)
      
        right_box = qtgui.QVBoxLayout()
        right_box.addWidget(good_button)
        right_box.addWidget(weak_button)
        right_box.addWidget(bad_button)
        right_box.addWidget(ephem_button)
        right_box.addWidget(nodetect_button)
        right_box.addWidget(zap_button)
        right_box.addWidget(skip_button)
        right_box.addWidget(reload_button)
        right_box.addWidget(priority_button)
        right_box.addWidget(addparents_button)
        right_box.addStretch(1)
        right_box.addWidget(qtgui.QLabel("Num left:"))
        right_box.addWidget(self.lcd)

        main_box = qtgui.QHBoxLayout()
        main_box.addLayout(left_box)
        main_box.addLayout(right_box)

        self.setLayout(main_box)

    def set_file_as_good(self, note=None):
        if self.file_id:
            values = {'qcpassed': True}
            if self.fileinfo['stage'] == 'calibrated':
                values['status'] = 'toload'
            if note == 'weak':
                values['note'] = "A weak detection."
            elif note == 'ephem':
                values['note'] = "Ephemeris needs to be updated."
            else:
                values['note'] = None

            with self.db.transaction() as conn:
                now = datetime.datetime.now()
                update = self.db.files.update().\
                            where(self.db.files.c.file_id == self.file_id).\
                            values(last_modified=now)
                conn.execute(update, values)
                note = values['note']
                if note is None:
                    note = "Passed QC"
                insert = self.db.qctrl.insert().\
                            values(file_id=self.file_id,
                                   obs_id=self.fileinfo['obs_id'],
                                   user=os.getlogin(),
                                   qcpassed=True,
                                   note=note,
                                   added=now,
                                   last_modified=now)
                conn.execute(insert)
            self.advance_file()

    def set_file_as_bad(self, reason='rfi'):
        if self.file_id:
            note = "File failed quality control."
            if reason == 'rfi':
                note += " RFI has rendered the observation unsalvageable!"
            elif reason == 'nondetect':
                note += " The observation is a non-detection."
            else:
                raise errors.UnrecognizedValueError("The reason for "
                                                    "setting the file as "
                                                    "bad is not "
                                                    "recognized: %s" %
                                                    reason)
            with self.db.transaction() as conn:
                now = datetime.datetime.now()
                update = self.db.files.update().\
                            where(self.db.files.c.file_id == self.file_id).\
                            values(qcpassed=False,
                                    status='failed',
                                    note=note,
                                    last_modified=now)
                conn.execute(update)
                insert = self.db.qctrl.insert().\
                            values(file_id=self.file_id,
                                   obs_id=self.fileinfo['obs_id'],
                                   user=os.getlogin(),
                                   qcpassed=False,
                                   note=note,
                                   added=now,
                                   last_modified=now)
                conn.execute(insert)
                if self.fileinfo['stage'] == 'calibrated':
                    # Mark former cleaned file to be loaded
                    ancestors = reduce_data.get_all_ancestors(self.file_id, self.db)
                    for ancestor in ancestors:
                        if ancestor['stage'] == 'cleaned':
                            ancestor_file_id = ancestor['file_id']
                            break
                    update = self.db.files.update().\
                                where((self.db.files.c.file_id == ancestor_file_id)).\
                                values(status='toload',
                                       note="Derived calibrated file failed quality control.",
                                       last_modified=datetime.datetime.now())
                    conn.execute(update)
                    # Update observation's current file
                    update = self.db.obs.update().\
                                where((self.db.obs.c.obs_id == self.fileinfo['obs_id'])).\
                                values(current_file_id=ancestor_file_id,
                                       last_modified=datetime.datetime.now())
                    conn.execute(update)
            self.advance_file()

    def advance_file(self):
        self.idiag = 0
        if self.files_to_check:
            file_id = self.files_to_check.pop()
            self.set_file(file_id)
            # Decrement number of files left
            self.lcd.display(len(self.files_to_check)+1)
        else:
            self.file_id = None
            self.diagplots = []
            self.image_holder.setText("No files on stack...")
            self.lcd.display(0)

    def cycle_diag_fwd(self):
        if self.diagplots:
            self.idiag = (self.idiag + 1) % len(self.diagplots)
            self.display_file()

    def cycle_diag_rev(self):
        if self.diagplots:
            self.idiag = (self.idiag - 1) % len(self.diagplots)
            self.display_file()

    def display_file(self):
        # Display diagnostic
        diagfn = self.diagplots[self.idiag]
        image = qtgui.QPixmap(diagfn)
        self.image_holder.setPixmap(image)
        # Display text information
        self.plot_lbl.setText(os.path.basename(diagfn))

    def set_file(self, file_id):
        if self.file_id != file_id:
            amsp.make_and_load_diagnostics(file_id, self.db)
            with self.db.transaction() as conn:
                # Get file information from DB
                select = self.db.select([self.db.files]).\
                        where(self.db.files.c.file_id == file_id)
                result = conn.execute(select)
                rows = result.fetchall()
                if len(rows) != 1:
                    raise errors.DatabaseError("Bad number of rows (%d) "
                                               "with file_id=%d!" %
                                               (len(rows), file_id))
                ff = rows[0]
                # Get diagnostics from DB
                select = self.db.select([self.db.diagnostics]).\
                        where(self.db.diagnostics.c.file_id == file_id)
                result = conn.execute(select)
                rows = result.fetchall()
                if len(rows) == 0:
                    raise errors.DiagnosticError("No diagnostics for "
                                                 "file (ID: %d) '%s'!" %
                                                 (file_id,
                                                  os.path.join(ff['filepath'],
                                                               ff['filename'])))
                self.diagplots = [os.path.join(row['diagnosticpath'],
                                  row['diagnosticname']) for row in rows]
                self.idiag = 0
            self.file_id = file_id
            self.fileinfo = ff
            self.added_parent_plots = False
            self.added_cal_plots = False
            self.display_file()
   
    def write_filename(self):
        ff = self.fileinfo
        if self.file_id is not None:
            print os.path.join(ff['filepath'], ff['filename'])

    def add_parents_diags(self):
        ff = self.fileinfo
        file_id = ff['file_id']
        if not self.added_parent_plots and self.file_id is not None:
            parent_file_id = ff['parent_file_id']
            if parent_file_id is not None:
                with self.db.transaction() as conn:
                    # Get diagnostics from DB
                    select = self.db.select([self.db.diagnostics]).\
                            where(self.db.diagnostics.c.file_id == parent_file_id)
                    result = conn.execute(select)
                    rows = result.fetchall()
                if len(rows) == 0:
                    raise warnings.warn("No diagnostics for "
                                        "file (ID: %d) '%s'!" %
                                        (file_id, os.path.join(ff['filepath'],
                                                               ff['filename'])),
                                        errors.CoastGuardWarning)
                self.diagplots.extend([os.path.join(row['diagnosticpath'],
                                    row['diagnosticname']) for row in rows])
            self.added_parents_plots = True

    def add_cal_diags(self):
        ff = self.fileinfo
        file_id = ff['file_id']
        if not self.added_cal_plots and self.file_id is not None:
            cal_file_id = ff['cal_file_id']
            if cal_file_id is not None:
                with self.db.transaction() as conn:
                    # Get diagnostics from DB
                    select = self.db.select([self.db.diagnostics]).\
                            where(self.db.diagnostics.c.file_id == cal_file_id)
                    result = conn.execute(select)
                    rows = result.fetchall()
                if len(rows) == 0:
                    raise warnings.warn("No diagnostics for "
                                        "file (ID: %d) '%s'!" %
                                        (file_id, os.path.join(ff['filepath'],
                                                               ff['filename'])),
                                        errors.CoastGuardWarning)
                self.diagplots.extend([os.path.join(row['diagnosticpath'],
                                                    row['diagnosticname'])
                                       for row in rows])
            self.added_cal_plots = True

    def get_files_to_check(self, priorities=None, stage=None, re_eval=None):
        if stage is None:
            stage = self.stage
        if re_eval is None:
            re_eval = self.re_eval
        if priorities is None:
            priorities = self.priorities
        
        whereclause = (self.db.files.c.status == 'new')
        if stage == 'cleaned':
            whereclause &= (self.db.files.c.stage == 'cleaned')
        elif stage == 'calibrated':
            whereclause &= (self.db.files.c.stage == 'calibrated') & \
                           (self.db.obs.c.obstype == 'pulsar')
        if not re_eval:
            whereclause &= (self.db.files.c.qcpassed.is_(None))
        
        if priorities:
            priority_list = []
            for pr in priorities:
                priority_list.extend(reduce_data.parse_priorities(pr))
            prioritizer, cfgstr = priority_list[0]
            tmp = prioritizer(self.db, cfgstr)
            for prioritizer, cfgstr in priority_list[1:]:
                tmp |= prioritizer(self.db, cfgstr)
            whereclause &= tmp
        with self.db.transaction() as conn:
            select = self.db.select([self.db.files.c.file_id],
                        from_obj=[self.db.obs.\
                            outerjoin(self.db.files,
                                onclause=self.db.files.c.file_id ==
                                        self.db.obs.c.current_file_id)]).\
                        where(whereclause).\
                        order_by(self.db.obs.c.obstype.asc())
            result = conn.execute(select)
            rows = result.fetchall()
            result.close()
        self.files_to_check = [row['file_id'] for row in rows]
#        self.files_to_check.sort()
#        self.files_to_check.reverse()
        self.advance_file()

    def zap_file_manually(self, reset_weights=False):
        arfn = os.path.join(self.fileinfo['filepath'],
                            self.fileinfo['filename'])
        arf = utils.ArchiveFile(arfn) 
        zapdialog = ZappingDialog()
        zapdialog.show()
        # This blocks input to the main quality control window
        out = zapdialog.zap(arfn, reset_weights)
        if out is not None and os.path.isfile(out):
            # Successful! Insert entry into DB.
            outdir, outfn = os.path.split(out)
            values = {'filepath': outdir,
                      'filename': outfn,
                      'stage': 'cleaned',
                      'note': "Manually zapped",
                      'qcpassed': None,
                      'status': 'new',
                      'snr': utils.get_archive_snr(out),
                      'md5sum': utils.get_md5sum(out),
                      'coords': self.fileinfo['coords'],
                      'ephem_md5sum': self.fileinfo['ephem_md5sum'],
                      'filesize': os.path.getsize(out),
                      'parent_file_id': self.file_id}

            if self.fileinfo['stage'] == 'calibrated':
                values['stage'] = 'calibrated'
                values['cal_file_id'] = self.fileinfo['cal_file_id']
            with self.db.transaction() as conn:
                version_id = utils.get_version_id(self.db)
                # Insert new entry
                insert = self.db.files.insert().\
                        values(version_id=version_id,
                                obs_id=self.fileinfo['obs_id'])
                result = conn.execute(insert, values)
                file_id = result.inserted_primary_key[0]
                # Update parent file's entry
                update = self.db.files.update().\
                        where(self.db.files.c.file_id == self.file_id).\
                        values(qcpassed=False,
                                status='replaced',
                                note="File had to be cleaned by hand.",
                                last_modified=datetime.datetime.now())
                result = conn.execute(update)
                # Update current file for observation
                update = self.db.obs.update().\
                            where(self.db.obs.c.obs_id == self.fileinfo['obs_id']).\
                            values(current_file_id=file_id,
                                   last_modified=datetime.datetime.now())
                conn.execute(update)
            self.advance_file()       

    def set_priorities(self):
        if self.priorities:
            curr_priority_str = ", ".join(self.priorities)
        else:
            curr_priority_str = ""
        priority_str, ok = qtgui.QInputDialog.getText(self,
                                                      "Set priorities",
                                                      "Enter a comma-"
                                                      "separated list of "
                                                      "priorities.",
                                                      qtgui.QLineEdit.Normal,
                                                      curr_priority_str)
        priority_str = str(priority_str)
        if ok:
            if not priority_str.strip():
                # Match all source names
                self.priorities = None
            else:
                self.priorities = [pr.strip() for pr in
                                   priority_str.split(',')]
        self.get_files_to_check()


class ZappingDialog(qtgui.QDialog):
    def __init__(self):
        super(ZappingDialog, self).__init__()
        # Set up with dialog
        self.__setup()
        self.__add_widgets()
        self.activateWindow()
        self.raise_()

    def __setup(self):
        self.setWindowTitle("Zapping...")
        self.setModal(True)
        self.setVisible(True)

    def __add_widgets(self):
        lbl = qtgui.QLabel()
        lbl.setText("<b>Running pzrzap...</b>")
        self.textedit = qtgui.QPlainTextEdit()
        self.textedit.setReadOnly(True)
        self.textedit.setFont(qtgui.QFont("Courier"))

        main_layout = qtgui.QVBoxLayout()
        main_layout.addWidget(lbl)
        main_layout.addWidget(self.textedit)
        self.setLayout(main_layout)

    def __on_finish(self, exitcode):
        msg_dialog = qtgui.QMessageBox()
        insize = os.path.getsize(self.infn)
        outsize = os.path.getsize(self.outfn)
        if exitcode != 0:
            success = False
            msg_dialog.setText("Zapping failed!")
        elif not outsize:
            success = False
            msg_dialog.setText("Output zapped file is empty.")
            msg_dialog.setInformativeText("Did you forget to save?")
            msg_dialog.setStandardButtons(qtgui.QMessageBox.Yes |
                                          qtgui.QMessageBox.No)
            msg_dialog.setDefaultButton(qtgui.QMessageBox.Yes)
        elif outsize != insize:
            success = True
            msg_dialog.setText("Output zapped file's size (%d) is "
                               "different than input file's size "
                               "(%s: %d bytes)" %
                               (outsize, self.infn, insize))
            msg_dialog.setInformativeText("Save zapped file?")
            msg_dialog.setStandardButtons(qtgui.QMessageBox.Save |
                                          qtgui.QMessageBox.Discard)
            msg_dialog.setDefaultButton(qtgui.QMessageBox.Discard)
        else:
            success = True
            msg_dialog.setText("The archive has been zapped.")
            msg_dialog.setInformativeText("Save zapped file?")
            msg_dialog.setStandardButtons(qtgui.QMessageBox.Save |
                                          qtgui.QMessageBox.Discard)
            msg_dialog.setDefaultButton(qtgui.QMessageBox.Save)
        ret = msg_dialog.exec_()
        if not success:
            pass
        elif ret == qtgui.QMessageBox.Save:
            pass
        elif ret == qtgui.QMessageBox.Discard:
            success = False
        else:
            raise ValueError("Value returned by message dialog (%d) "
                             "does not match the Save or Discard "
                             "buttons!" % ret)
        self.done(success)

    def __on_stderr(self, text=None):
        if text is None:
            stderr_data = self.proc.readAllStandardError()
            text = qtcore.QString(stderr_data)
        self.textedit.appendPlainText(text)

    def __on_stdout(self, text=None):
        if text is None:
            stdout_data = self.proc.readAllStandardOutput()
            text = qtcore.QString(stdout_data)
        self.textedit.appendPlainText(text)

    def zap(self, arfn, reset_weights=False):
        self.setWindowTitle("Zapping %s..." % os.path.basename(arfn))
        # Create temporary file for output
        tmpdir = os.path.join(config.tmp_directory, 'qctrl')
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)
        tmpfile, tmpoutfn = tempfile.mkstemp(suffix=".ar", dir=tmpdir)
        os.close(tmpfile)  # Close open file handle
        try:
            success = self.__launch_zapping(arfn, tmpoutfn, reset_weights)
            if success:
                arf = utils.ArchiveFile(arfn)
                archivedir = os.path.join(config.output_location,
                                          config.output_layout) % arf
                # Append .zap to filename
                archivefn = (os.path.basename(arfn)+".zap") % arf
                outfn = os.path.join(archivedir, archivefn)
                # Ensure group has write permission to this file
                # NOT SURE THIS IS NECESSARY
                #utils.add_group_permissions(tmpoutfn, 'w')
                shutil.move(tmpoutfn, outfn)
                return outfn
            else:
                return None
        finally:
            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir)
    
    def __launch_zapping(self, infn, outfn, reset_weights=False):
        self.infn = infn
        self.outfn = outfn
        self.proc = qtcore.QProcess()
        self.proc.readyReadStandardOutput.connect(self.__on_stdout)
        self.proc.readyReadStandardError.connect(self.__on_stderr)
        if reset_weights:
            self.__on_stdout("Resetting profile weights")
            tmpfn = outfn+".in"
            shutil.copy(infn, tmpfn)
            infn = tmpfn
            self.proc.start('pam', ['-m', '-w', '1', infn])
            self.proc.waitForFinished(msecs=-1)  # Block until finished
            self.__on_stdout("Done")
        self.proc.finished.connect(self.__on_finish)
        self.proc.start('pzrzap', [infn, '-o', outfn])
        success = self.exec_()
        return success


def main():
    app = qtgui.QApplication(sys.argv)
    
    qctrl_win = QualityControl(priorities=args.priority, stage=args.stage,
                               re_eval=args.re_eval)
    qctrl_win.get_files_to_check()
    # Display the window
    qctrl_win.show()

    exitcode = app.exec_()
    sys.exit(exitcode)


if __name__ == "__main__":
    parser = utils.DefaultArguments(description="Quality control interface "
                                    "for Asterix data.")
    parser.add_argument("--prioritize", action='append',
                        default=[], dest='priority',
                        help="A rule for prioritizing observations.")
    parser.add_argument('-C', "--calibrated", dest='stage', action='store_const',
                        default='cleaned', const='calibrated',
                        help="Review calibrated pulsar observations.")
    parser.add_argument('-R', "--re-eval", dest='re_eval', action='store_true',
                        help="Review files with status 'new' even if they already "
                             "have a quality control assessment.")
    args = parser.parse_args()
    main()

