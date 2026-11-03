Release Policy

This document outlines the release lifecycle, versioning strategy, and deprecation policy for Tau.

1. Versioning Strategy

Tau adheres to Semantic Versioning (SemVer), expressed as MAJOR.MINOR.PATCH:

MAJOR (X.0.0): Incremented for incompatible API changes, breaking architectural shifts, or major structural overhauls.

MINOR (0.X.0): Incremented for backwards-compatible new features, performance enhancements, or non-breaking functional additions.

PATCH (0.0.X): Incremented for backwards-compatible bug fixes, security patches, and minor documentation corrections.

2. Release Cadence & Lifecycle

We follow a predictable release cadence to ensure stability and regular feature delivery:

Patch Releases: Issued on an as-needed basis for critical bug fixes and security vulnerabilities.

Minor Releases: Scheduled every quarter containing accumulated features and improvements.

Major Releases: Scheduled annually or as dictated by significant architectural evolution.

Release Branches & Support Status

Current Stable (main / Latest Tag): Actively maintained with new features, bug fixes, and security patches.

LTS / Maintenance Branch: For major versions designated as Long-Term Support, we provide critical bug fixes and security patches for 12 months following the initial release.

3. Pre-Releases and Testing

Before any official production release, code undergoes rigorous testing phases:

Alpha (-alpha.X): Early development builds. APIs may change rapidly; not recommended for production use.

Beta (-beta.X): Feature-complete builds undergoing user testing and integration validation. Core APIs are stable.

Release Candidate (-rc.X): Final stabilization phase prior to General Availability (GA). Only critical bug fixes are merged.

4. Deprecation Policy

We strive to maintain high backward compatibility, but evolution requires deprecating outdated features:

Notification: Features or APIs intended for deprecation will be clearly marked as @deprecated (or equivalent) in the documentation and release notes at least one minor version prior to removal.

Removal: Deprecated features will only be removed in a MAJOR version release.

5. Changelog & Release Notes

Every release is accompanied by detailed release notes published on our Releases Page, detailing:

New Features

Bug Fixes

Breaking Changes & Migration Guides
