const assert = require('node:assert/strict');
const test = require('node:test');
const Module = require('node:module');

test('preload exposes desktop version from BrowserWindow additionalArguments', (t) => {
  const originalLoad = Module._load;
  const originalArgv = [...process.argv];
  const exposeInMainWorldCalls = [];
  const expectedVersion = '3.12.0';
  const ipcRenderer = {
    invoke: () => undefined,
    on: () => undefined,
    removeListener: () => undefined,
  };

  Module._load = function patchedLoad(request, parent, isMain) {
    if (request === 'electron') {
      return {
        contextBridge: {
          exposeInMainWorld: (...args) => {
            exposeInMainWorldCalls.push(args);
          },
        },
        ipcRenderer,
      };
    }
    return originalLoad.call(this, request, parent, isMain);
  };

  const preloadPath = require.resolve('../preload.js');
  delete require.cache[preloadPath];
  process.argv = [...originalArgv, `--dsa-desktop-version=${expectedVersion}`];

  t.after(() => {
    Module._load = originalLoad;
    process.argv = originalArgv;
    delete require.cache[preloadPath];
  });

  const preloadModule = require('../preload.js');

  assert.equal(exposeInMainWorldCalls.length, 1);
  assert.equal(exposeInMainWorldCalls[0][0], 'dsaDesktop');
  assert.equal(exposeInMainWorldCalls[0][1].version, expectedVersion);
  assert.equal(typeof exposeInMainWorldCalls[0][1].getUpdateState, 'function');
  assert.equal(typeof exposeInMainWorldCalls[0][1].checkForUpdates, 'function');
  assert.equal(typeof exposeInMainWorldCalls[0][1].openReleasePage, 'function');
  assert.equal(typeof exposeInMainWorldCalls[0][1].onUpdateStateChange, 'function');
  assert.equal(
    preloadModule.readDesktopVersion([`--dsa-desktop-version=${expectedVersion}`]),
    expectedVersion
  );
});

test('preload falls back to empty version when BrowserWindow does not pass one', (t) => {
  const originalLoad = Module._load;
  const originalArgv = [...process.argv];
  const exposeInMainWorldCalls = [];
  const ipcRenderer = {
    invoke: () => undefined,
    on: () => undefined,
    removeListener: () => undefined,
  };

  Module._load = function patchedLoad(request, parent, isMain) {
    if (request === 'electron') {
      return {
        contextBridge: {
          exposeInMainWorld: (...args) => {
            exposeInMainWorldCalls.push(args);
          },
        },
        ipcRenderer,
      };
    }
    return originalLoad.call(this, request, parent, isMain);
  };

  const preloadPath = require.resolve('../preload.js');
  delete require.cache[preloadPath];
  process.argv = originalArgv.filter((value) => !value.startsWith('--dsa-desktop-version='));

  t.after(() => {
    Module._load = originalLoad;
    process.argv = originalArgv;
    delete require.cache[preloadPath];
  });

  const preloadModule = require('../preload.js');

  assert.equal(exposeInMainWorldCalls.length, 1);
  assert.equal(exposeInMainWorldCalls[0][0], 'dsaDesktop');
  assert.equal(exposeInMainWorldCalls[0][1].version, '');
  assert.equal(preloadModule.readDesktopVersion(['--unrelated=1']), '');
});
