import type { CapacitorConfig } from '@capacitor/cli';

const config: CapacitorConfig = {
  appId: 'com.ncto.dashboard',
  appName: 'NCTO Dashboard',
  webDir: 'www',
  server: {
    url: 'https://dashboard.ncto.gov.ng',
    cleartext: false
  },
  ios: {
    contentInset: 'automatic'
  },
  android: {
    allowMixedContent: false
  }
};

export default config;
