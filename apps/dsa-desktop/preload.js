const { contextBridge, ipcRenderer } = require('electron');

const DESKTOP_VERSION_ARG_PREFIX = '--dsa-desktop-version=';

function readDesktopVersion(argv = process.argv) {
  const versionArg = argv.find(
    (value) => typeof value === 'string' && value.startsWith(DESKTOP_VERSION_ARG_PREFIX)
  );
  return versionArg ? versionArg.slice(DESKTOP_VERSION_ARG_PREFIX.length) : '';
}

contextBridge.exposeInMainWorld('dsaDesktop', {
  version: readDesktopVersion(),
  getUpdateState: () => ipcRenderer.invoke('desktop:get-update-state'),
  checkForUpdates: () => ipcRenderer.invoke('desktop:check-for-updates'),
  openReleasePage: (url) => ipcRenderer.invoke('desktop:open-release-page', url),
  onUpdateStateChange: (handler) => {
    if (typeof handler !== 'function') {
      return () => undefined;
    }

    const listener = (_event, payload) => {
      handler(payload);
    };
    ipcRenderer.on('desktop:update-state-changed', listener);
    return () => {
      ipcRenderer.removeListener('desktop:update-state-changed', listener);
    };
  },
});

module.exports = {
  DESKTOP_VERSION_ARG_PREFIX,
  readDesktopVersion,
};
