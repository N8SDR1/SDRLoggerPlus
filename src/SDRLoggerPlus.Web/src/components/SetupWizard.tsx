import { useState } from 'react';
import { CheckCircle, Loader2, HardDrive, Check, Radio } from 'lucide-react';
import { useSetupStore } from '../store/setupStore';

interface SetupWizardProps {
  onComplete: () => void;
}

// SDRLoggerPlus uses a local LiteDB database only. This first-run screen just
// confirms the local database and starts the app.
export function SetupWizard({ onComplete }: SetupWizardProps) {
  const { configureLocal, isLoading, error, clearError } = useSetupStore();
  const [done, setDone] = useState(false);

  const handleLocalSetup = async () => {
    clearError();
    const result = await configureLocal();
    if (result?.success) {
      setDone(true);
      setTimeout(onComplete, 1200);
    }
  };

  return (
    <div className="fixed inset-0 bg-dark-900 flex items-center justify-center z-[200]">
      <div className="glass-panel w-full max-w-lg mx-4 animate-fade-in border border-glass-200 rounded-xl shadow-2xl">
        {/* Header */}
        <div className="p-6 border-b border-glass-100">
          <div className="flex items-center gap-4">
            <div className="w-14 h-14 bg-orange-500/20 rounded-xl flex items-center justify-center">
              <Radio className="w-8 h-8 text-orange-500" />
            </div>
            <div>
              <h1 className="text-2xl font-bold text-orange-500">SDRLOGGERPLUS</h1>
              <p className="text-sm text-gray-400">Welcome! Let's get you set up.</p>
            </div>
          </div>
        </div>

        {/* Content */}
        <div className="p-6">
          {done ? (
            <div className="text-center py-8">
              <CheckCircle className="w-16 h-16 text-green-400 mx-auto mb-4" />
              <h2 className="text-xl font-semibold text-gray-100 mb-2">You're all set!</h2>
              <p className="text-gray-400">Using local database</p>
            </div>
          ) : (
            <div className="space-y-6">
              <div className="p-6 rounded-xl border border-accent-success/30">
                <div className="flex flex-col items-center text-center">
                  <HardDrive className="w-10 h-10 text-accent-success mb-4" />
                  <h3 className="text-lg font-semibold text-dark-200 mb-3">Local Database</h3>
                  <ul className="space-y-2 text-sm text-dark-300 mb-6">
                    <li className="flex items-center gap-2">
                      <Check className="w-4 h-4 text-accent-success flex-shrink-0" />
                      Works offline
                    </li>
                    <li className="flex items-center gap-2">
                      <Check className="w-4 h-4 text-accent-success flex-shrink-0" />
                      No setup needed
                    </li>
                    <li className="flex items-center gap-2">
                      <Check className="w-4 h-4 text-accent-success flex-shrink-0" />
                      Data stays on this computer
                    </li>
                  </ul>
                </div>
                <button
                  onClick={handleLocalSetup}
                  disabled={isLoading}
                  className="w-full glass-button-success px-6 py-2.5 flex items-center justify-center gap-2 disabled:opacity-50"
                >
                  {isLoading ? (
                    <>
                      <Loader2 className="w-4 h-4 animate-spin" />
                      Setting up...
                    </>
                  ) : (
                    'Get Started'
                  )}
                </button>
              </div>

              {error && (
                <div className="p-4 rounded-lg border bg-red-500/10 border-red-500/30">
                  <p className="text-red-400">{error}</p>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
