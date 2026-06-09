# @terion-name/air3-edgesign

TypeScript helpers for signing and verifying air3 edge gateway URLs.

This package is published publicly to GitHub Packages as `@terion-name/air3-edgesign`. Configure npm for the `@terion-name` scope, authenticate if GitHub's npm registry requires it, then install:

```sh
# One-time user config; alternatively put this line in your project .npmrc.
npm config set @terion-name:registry https://npm.pkg.github.com

# Use a classic PAT with read:packages when prompted.
npm login --scope=@terion-name --registry=https://npm.pkg.github.com

npm install @terion-name/air3-edgesign
```

For CI, set `NODE_AUTH_TOKEN` (for example from `${GITHUB_TOKEN}` in GitHub Actions, or another token with package read access) and add a project `.npmrc` like:

```ini
@terion-name:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${NODE_AUTH_TOKEN}
```

```ts
import { signUrl, verifyUrl } from '@terion-name/air3-edgesign';
```

This package is published to GitHub Packages by the tag release workflow. The published runtime entry point is built JavaScript in `dist/` with TypeScript declarations.
