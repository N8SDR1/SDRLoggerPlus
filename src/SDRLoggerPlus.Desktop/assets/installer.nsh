; SDRLoggerPlus custom NSIS hooks for electron-builder.
; Compiled into BOTH the installer and the uninstaller (shared header).
; LogicLib and the runtime ${isUpdated} flag are provided by the generated
; electron-builder script before this file is included.

; ---------------------------------------------------------------------------
; Kill the spawned .NET backend. electron-builder's CHECK_APP_RUNNING only
; manages SDRLoggerPlus.exe; its taskkill /f fallback bypasses Electron's
; before-quit cleanup and orphans SDRLoggerPlus.Server.exe, which holds locks on
; $INSTDIR files and on sdrloggerplus.db. /F from the start: the backend is a
; windowless console process and rejects graceful termination. The USERNAME
; filter keeps an admin's per-machine uninstall from killing another logged-in
; user's backend.
; ---------------------------------------------------------------------------
!macro qtKillBackend
  ReadEnvStr $R8 "USERNAME"
  nsExec::Exec 'taskkill /F /IM "SDRLoggerPlus.Server.exe" /FI "USERNAME eq $R8"'
  Pop $R8
  Sleep 1000
!macroend

; ---------------------------------------------------------------------------
; Default install directory: C:\SDRLoggerPlus (user's preference; still changeable
; in the wizard). electron-builder's multiUser init takes the default from the
; InstallLocation registry value when present, so seed it in preInit — the
; documented electron-builder recipe for a custom default install dir. A prior
; install's own InstallLocation simply overwrites this same value afterwards.
; ---------------------------------------------------------------------------
!macro preInit
  SetRegView 64
  WriteRegExpandStr HKCU "${INSTALL_REGISTRY_KEY}" InstallLocation "C:\SDRLoggerPlus"
!macroend

!macro customInit
  !insertmacro qtKillBackend
!macroend

!macro customUnInit
  ; The template's un.onInit just ran SetOutPath $INSTDIR (uninstaller.nsh:6),
  ; parking this process's working directory inside the very directory the
  ; un.install section later RMDir /r's. Windows will not remove a directory
  ; that is a live process's CWD (and launching the uninstaller from Explorer
  ; inherits an $INSTDIR CWD as well), so the emptied root survives as a husk.
  ; Park our CWD somewhere neutral before any deletion happens.
  SetOutPath "$TEMP"
  !insertmacro qtKillBackend
!macroend

; ---------------------------------------------------------------------------
; Data-deletion prompt. electron-builder 26.x inserts customUnInstall at the
; START of the uninstall section (before app-file removal) — placement is
; order-independent for us since we only touch %APPDATA%.
; Never deletes on update (${isUpdated} is a runtime test of the --updated
; switch; upgrades always run the old uninstaller with /S --updated) and never
; on silent uninstall. MB_DEFBUTTON2 is load-bearing: plain MB_YESNO defaults
; to Yes, and Enter must NOT delete the QSO database. /SD IDNO is redundant
; behind the Silent guard — kept as belt-and-suspenders.
; ---------------------------------------------------------------------------
!macro customUnInstall
  ${ifNot} ${isUpdated}
    ${ifNot} ${Silent}
      IfFileExists "$APPDATA\SDRLoggerPlus\*.*" 0 qtDataDone
      MessageBox MB_YESNO|MB_ICONQUESTION|MB_DEFBUTTON2 \
        "Also delete your SDRLoggerPlus data folder?$\r$\n$\r$\nThis permanently deletes your QSO database, settings, and any backups stored in the SDRLoggerPlus data folder ($APPDATA\SDRLoggerPlus).$\r$\n$\r$\nBackups saved to other locations are not touched." \
        /SD IDNO IDYES qtDoDelete
      Goto qtDataDone

qtDoDelete:
      ; Per-machine uninstalls run with SetShellVarContext all, where $APPDATA
      ; is C:\ProgramData — switch to the user context around the delete
      ; (same dance as electron-builder's own delete-app-data block).
      ${if} $installMode == "all"
        SetShellVarContext current
      ${endIf}

      ; Junction guard: NSIS RMDir /r recurses THROUGH directory junctions and
      ; deletes the target's contents. If the data folder root or backups\ is
      ; a reparse point, remove the link itself (plain RMDir) and never recurse
      ; into it. GetFileAttributes returns -1 for a missing path; -1 & 0x400 is
      ; nonzero, so a missing path harmlessly takes the plain-RMDir branch.
      System::Call 'kernel32::GetFileAttributes(t "$APPDATA\SDRLoggerPlus") i .R7'
      IntOp $R7 $R7 & 0x400
      ${if} $R7 <> 0
        RMDir "$APPDATA\SDRLoggerPlus"
      ${else}
        System::Call 'kernel32::GetFileAttributes(t "$APPDATA\SDRLoggerPlus\backups") i .R7'
        IntOp $R7 $R7 & 0x400
        ${if} $R7 <> 0
          RMDir "$APPDATA\SDRLoggerPlus\backups"
        ${endIf}
        RMDir /r "$APPDATA\SDRLoggerPlus"
      ${endIf}

      ${if} $installMode == "all"
        SetShellVarContext all
      ${endIf}

qtDataDone:
    ${endIf}
  ${endIf}

  ; Husk note: electron-builder 26.x's template does SetOutPath $TEMP itself
  ; right before RMDir /r $INSTDIR, so the empty-root-left-behind bug that
  ; existed under 25.x (CWD parked in $INSTDIR) is fixed natively; our
  ; customUnInit $TEMP park above remains as defense in depth.
!macroend
