(() => {
    const table = document.querySelector("#PackageTable_dataTable");
    const headerRow = document.querySelector("#PackageTable_headerTable");

    if (!table || !headerRow) return console.error("Table or headers not found.");

    // Get headers and remove empty ones
    const headers = [...headerRow.querySelectorAll("td, th")]
        .map(cell => cell.innerText.trim())
        .filter(text => text);

    const data = [...table.querySelectorAll("tbody tr")].map(row => {
        const cells = [...row.querySelectorAll("td")].map(cell => cell.innerText.trim()).slice(1); // Shift left by one

        return Object.fromEntries(cells.map((cell, i) => [headers[i], cell]));
    }).filter(row => Object.values(row).some(val => val)); // Remove empty rows

    // Convert data to CSV
    const csv = [
        headers.join(','), // Header row
        ...data.map(row => headers.map(header => row[header] || '').join(',')) // Data rows
    ].join('\n');

    // Create a Blob and download the CSV
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'data.csv';
    a.click();
    URL.revokeObjectURL(url); // Clean up
})();
