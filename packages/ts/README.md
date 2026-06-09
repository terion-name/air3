# @terion/air3-edgesign

TypeScript helpers for signing and verifying air3 edge gateway URLs.

This package is published publicly to npmjs as `@terion/air3-edgesign`:

```sh
npm install @terion/air3-edgesign
```

```ts
import { signUrl, verifyUrl } from '@terion/air3-edgesign';
```

This package is published by the tag release workflow using npm Trusted Publishing from `.github/workflows/release.yml`. The GitHub Actions job authenticates to npm through OIDC instead of an npm token. The published runtime entry point is built JavaScript in `dist/` with TypeScript declarations.
