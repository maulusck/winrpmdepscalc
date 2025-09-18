// ==UserScript==
// @name         EPEL 9 RPM Auto Downloader
// @namespace    https://yourdomain.example
// @version      1.0
// @description  Automatically downloads RPMs from EPEL 9 for a list of package names
// @author       ChatGPT
// @match        https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/Packages/*
// @grant        none
// ==/UserScript==

(function () {
  'use strict';

  const PACKAGES_KEY = 'epel9_rpm_packages';
  const INDEX_KEY = 'epel9_rpm_index';

  async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async function main() {
    // Ask for package list if not already stored
    if (!sessionStorage.getItem(PACKAGES_KEY)) {
      let input = prompt("Enter a comma-separated list of EPEL 9 RPM package names (e.g. nano,vim,libappindicator):");
      if (!input) {
        alert("No packages entered. Script will not run.");
        return;
      }
      const pkgs = input.split(',').map(p => p.trim()).filter(Boolean);
      if (pkgs.length === 0) {
        alert("No valid packages entered.");
        return;
      }
      sessionStorage.setItem(PACKAGES_KEY, JSON.stringify(pkgs));
      sessionStorage.setItem(INDEX_KEY, '0');
      alert(`Starting download of ${pkgs.length} packages...`);
    }

    const packages = JSON.parse(sessionStorage.getItem(PACKAGES_KEY));
    let index = parseInt(sessionStorage.getItem(INDEX_KEY), 10);

    if (index >= packages.length) {
      alert("All packages processed!");
      sessionStorage.removeItem(PACKAGES_KEY);
      sessionStorage.removeItem(INDEX_KEY);
      return;
    }

    const pkg = packages[index];
    const firstLetter = pkg[0].toLowerCase();
    const currentPath = window.location.pathname;
    const expectedPath = `/pub/epel/9/Everything/x86_64/Packages/${firstLetter}/`;

    // If not in the correct folder, redirect
    if (!currentPath.startsWith(expectedPath)) {
      const fullUrl = `https://dl.fedoraproject.org${expectedPath}`;
      console.log(`Navigating to directory for package "${pkg}"...`);
      window.location.href = fullUrl;
      return;
    }

    // We're in the right folder – search for RPM file
    const anchors = [...document.querySelectorAll('a')];
    const regex = new RegExp(`^${pkg}[-].*\\.rpm$`, 'i');
    const matchedLinks = anchors
      .map(a => a.getAttribute('href'))
      .filter(href => href && regex.test(href));

    if (matchedLinks.length === 0) {
      console.warn(`No RPM found for package "${pkg}"`);
      sessionStorage.setItem(INDEX_KEY, (index + 1).toString());
      location.reload();
      return;
    }

    // Download first match
    const rpmUrl = window.location.href + matchedLinks[0];
    console.log(`Downloading: ${rpmUrl}`);

    const a = document.createElement('a');
    a.href = rpmUrl;
    a.download = '';
    document.body.appendChild(a);
    a.click();
    a.remove();

    // Move to next package
    sessionStorage.setItem(INDEX_KEY, (index + 1).toString());

    await sleep(3000); // wait a bit before reloading
    location.reload();
  }

  main();
})();
