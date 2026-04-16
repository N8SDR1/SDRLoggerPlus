; ============================================================
; SDRLogger+ — Inno Setup Script
; ============================================================
; Prerequisites:
;   1. PyInstaller build must be complete:
;        pyinstaller SDRLoggerPlus.spec
;      Output folder:  hamlog\dist\SDRLoggerPlus\
;   2. Icon file at:   hamlog\static\img\sdrlogger.ico
;   3. Inno Setup 6+ installed: https://jrsoftware.org/isinfo.php
;
; To compile:
;   Open Inno Setup Compiler → File → Open → select this .iss
;   Click Build → Compile   (or press F9)
;
;   Output: installer\windows\Output\SDRLoggerPlus-Setup.exe
; ============================================================

#define AppName      "SDRLogger+"
#define AppVersion   "1.06"
#define AppPublisher "Rick N8SDR"
#define AppURL       "https://www.qrz.com/db/N8SDR"
#define AppExeName   "SDRLoggerPlus.exe"
#define AppDataName  "SDRLoggerPlus"

; Path to the PyInstaller output folder
; (relative to the location of this .iss file — adjust if you move it)
#define DistDir      "..\..\dist\SDRLoggerPlus"

; Path to the application icon
#define IconFile     "..\..\static\img\sdrlogger.ico"

[Setup]
AppId={{B2C3D4E5-F6A7-8901-BCDE-F12345678901}
AppName={#AppName}
AppVersion={#AppVersion}
AppVerName={#AppName} {#AppVersion}
AppPublisher={#AppPublisher}
AppPublisherURL={#AppURL}
AppSupportURL={#AppURL}
AppUpdatesURL={#AppURL}

; Default install location — user can change it during setup
DefaultDirName=C:\{#AppDataName}
DefaultGroupName={#AppName}
AllowNoIcons=yes

; Installer output
OutputDir=Output
OutputBaseFilename=SDRLoggerPlus-Setup-{#AppVersion}
SetupIconFile={#IconFile}
UninstallDisplayIcon={app}\{#AppExeName}

; Compression
Compression=lzma2/ultra64
SolidCompression=yes
LZMAUseSeparateProcess=yes

; Require admin rights so we can write to C:\SDRLoggerPlus
PrivilegesRequired=admin
PrivilegesRequiredOverridesAllowed=dialog

; Wizard appearance
WizardStyle=modern
WizardResizable=yes
DisableWelcomePage=no
DisableDirPage=no
DisableProgramGroupPage=no

; Minimum Windows version: 10
MinVersion=10.0

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon";      Description: "Create a &desktop shortcut";   GroupDescription: "Additional icons:"
Name: "startmenuicon";    Description: "Create a &Start Menu shortcut"; GroupDescription: "Additional icons:"
Name: "launchafterinstall"; Description: "&Launch SDRLogger+ when setup finishes"

[Files]
; Copy the entire PyInstaller dist folder into {app}
Source: "{#DistDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
; Change log — installed alongside the exe so users can review it
Source: "..\..\CHANGELOG.txt"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
; Desktop shortcut
Name: "{commondesktop}\{#AppName}";    Filename: "{app}\{#AppExeName}"; \
      IconFilename: "{app}\{#AppExeName}"; Comment: "Open SDRLogger+ Ham Radio Logger"; \
      Tasks: desktopicon

; Start Menu shortcut
Name: "{group}\{#AppName}";            Filename: "{app}\{#AppExeName}"; \
      IconFilename: "{app}\{#AppExeName}"; Comment: "Open SDRLogger+ Ham Radio Logger"; \
      Tasks: startmenuicon

; Uninstall entry in Start Menu
Name: "{group}\Uninstall {#AppName}";  Filename: "{uninstallexe}"

[Run]
; Optionally launch after install
Filename: "{app}\{#AppExeName}"; \
  Description: "Launch {#AppName} now"; \
  Flags: nowait postinstall skipifsilent; \
  Tasks: launchafterinstall

[UninstallDelete]
Type: dirifempty; Name: "{app}"

[Code]
// ── Custom welcome page text ──────────────────────────────────────────────
procedure InitializeWizard();
begin
  WizardForm.WelcomeLabel2.Caption :=
    'This will install SDRLogger+ v' + '{#AppVersion}' + ' on your computer.' + #13#10 + #13#10 +
    'SDRLogger+ is a Ham Radio contact logger designed for use with' + #13#10 +
    'Expert Electronics Thetis SDR, EESDR, or any HamLib-compatible' + #13#10 +
    'transceiver. Includes CW decoder, logbook uploads, DX cluster,' + #13#10 +
    'rotator control, POTA support, full S.A.T. satellite controller' + #13#10 +
    'integration, Awards tracking (DXCC, WAS, WAZ, WPX), and more.' + #13#10 + #13#10 +
    'Click Next to continue, or Cancel to exit Setup.';
end;

// ── Notify user if a previous installation is detected ───────────────────
function InitializeSetup(): Boolean;
begin
  Result := True;
  if DirExists(ExpandConstant('C:\{#AppDataName}')) then
  begin
    MsgBox(
      'A previous installation of SDRLogger+ was found.' + #13#10 + #13#10 +
      'Setup will overwrite program files only.' + #13#10 + #13#10 +
      'Your QSO database and settings are safe — they are stored in' + #13#10 +
      '%APPDATA%\SDRLoggerPlus and will NOT be changed.' + #13#10 + #13#10 +
      'Click OK to continue.',
      mbInformation, MB_OK);
  end;
end;

// ── On install: clear AppData templates + any zip-updated .py files ───────
// Databases (.db) and config.json are NEVER touched.
procedure CurStepChanged(CurStep: TSetupStep);
var
  TmplPath: string;
  DataPath: string;
  FindRec:  TFindRec;
begin
  if CurStep = ssInstall then
  begin
    DataPath := ExpandConstant('{userappdata}\SDRLoggerPlus');

    // Remove templates folder so launcher re-seeds it from the new bundle
    TmplPath := DataPath + '\templates';
    if DirExists(TmplPath) then
      DelTree(TmplPath, True, True, True);

    // Remove any .py files a zip updater may have placed in AppData
    if FindFirst(DataPath + '\*.py', FindRec) then
    begin
      try
        repeat
          DeleteFile(DataPath + '\' + FindRec.Name);
        until not FindNext(FindRec);
      finally
        FindClose(FindRec);
      end;
    end;
  end;
end;

// ── Post-install: browser opened automatically by launcher.py ────────────
