"""
CSS and JavaScript assets for HTML reports
"""


def _generate_css_styles() -> str:
    """Generate CSS styles for the HTML report"""
    return """
        :root {
            --text-main: #000;
            --text-muted: #666;
            --border-light: #e5e5e5;
            --accent: #6606dc;
            --bg-alt: #fafafa;
        }

        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #fff;
            color: var(--text-main);
            margin: 0;
            padding: 40px 24px;
            max-width: 1100px;
            margin-inline: auto;
            line-height: 1.6;
        }

        /* Header layout */
        .report-header {
            display: flex;
            flex-direction: column;
            gap: 16px;
            margin-bottom: 32px;
        }

        .report-title {
            font-size: 2.8rem;
            font-weight: 400;
            letter-spacing: -0.5px;
            line-height: 1.1;
            max-width: 900px;
            margin: 0;
        }

        .table-name {
            font-size: 2.2rem;
            font-weight: 700;
            line-height: 1;
            color: var(--accent);
            margin: 0;
        }

        .table-tag {
            font-size: 0.9rem;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: var(--text-muted);
            margin-top: 8px;
        }

        .meta-line {
            border-top: 1px solid var(--border-light);
            padding-top: 14px;
            display: flex;
            flex-wrap: wrap;
            gap: 24px;
            font-size: 0.9rem;
        }

        .meta-block {
            display: flex;
            flex-direction: column;
            align-items: flex-start;
            text-align: left;
        }

        .meta-label {
            font-size: 0.75rem;
            text-transform: uppercase;
            color: var(--text-muted);
            letter-spacing: 0.5px;
            text-align: left;
        }

        .summary-meta-value {
            font-size: 0.95rem;
            font-weight: 500;
            color: var(--accent);
            text-align: left;
        }

        /* Summary cards */
        .summary-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 24px;
            margin-top: 32px;
        }

        .card {
            display: flex;
            flex-direction: column;
            align-items: flex-start;
            gap: 4px;
        }

        .card h2 {
            font-size: 1.8rem;
            font-weight: 600;
            margin: 0;
            color: var(--accent);
        }

        .card p {
            font-size: 0.85rem;
            color: var(--text-muted);
            margin: 0;
        }

        .card .status-warning {
            color: #d97706;
        }

        /* Tabs */
        .tabs {
            display: flex;
            border-bottom: 1px solid var(--border-light);
            margin-top: 32px;
            gap: 32px;
        }

        .tab {
            position: relative;
            padding: 12px 0;
            font-size: 1rem;
            font-weight: 500;
            color: var(--text-muted);
            cursor: pointer;
            transition: color 0.2s ease;
            border: none;
            background: none;
        }

        .tab:hover {
            color: var(--text-main);
        }

        .tab.active {
            color: var(--accent);
        }

        .tab.active::after {
            content: "";
            position: absolute;
            bottom: -1px;
            left: 0;
            width: 100%;
            height: 2px;
            background: var(--accent);
        }

        /* Tab content */
        .tab-content {
            display: none;
            padding-top: 28px;
        }

        .tab-content.active {
            display: block;
        }

        /* Tables */
        .columns-table {
            margin-top: 16px;
            background: #fff;
            border: 1px solid var(--border-light);
            border-radius: 8px;
            box-shadow: 0 1px 4px rgba(0,0,0,0.04);
            overflow: hidden;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.95rem;
        }

        thead {
            background: var(--bg-alt);
        }

        th, td {
            padding: 12px 16px;
            text-align: left;
        }

        th {
            font-weight: 600;
            color: var(--text-muted);
            text-transform: uppercase;
            font-size: 0.8rem;
            letter-spacing: 0.5px;
        }

        tbody tr:nth-child(even) {
            background: #fcfcfc;
        }

        tbody tr:hover {
            background: #f8f5ff !important;
        }

        td {
            color: var(--text-main);
            font-weight: 400;
        }

        /* Column cards */
        .column-card {
            background: white;
            border: 1px solid var(--border-light);
            border-radius: 8px;
            margin-bottom: 16px;
            overflow: hidden;
            box-shadow: 0 1px 4px rgba(0,0,0,0.04);
        }

        .column-name {
            font-size: 1.2em;
            font-weight: 600;
            color: var(--text-main);
        }

        .column-type {
            background: var(--bg-alt);
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            color: var(--text-muted);
            font-weight: 500;
        }

        /* Issues */
        .summary {
            background: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 24px;
            border: 1px solid var(--border-light);
        }

        .summary.success {
            border-left: 4px solid #10b981;
        }

        .summary.warning {
            border-left: 4px solid #d97706;
        }

        .summary h2 {
            margin: 0 0 8px 0;
            font-size: 1.3rem;
            font-weight: 600;
        }

        .summary p {
            margin: 0;
            color: var(--text-muted);
        }

        .issue {
            background: #fffbeb;
            border-left: 3px solid #d97706;
            padding: 12px 16px;
            margin: 12px 0;
            border-radius: 4px;
        }

        .issue-type {
            font-weight: 600;
            color: #d97706;
            margin-bottom: 8px;
            font-size: 0.95rem;
        }

        .issue-stats {
            color: var(--text-muted);
            margin: 5px 0;
            font-size: 0.9rem;
        }

        .suggestion {
            background: #eff6ff;
            border-left: 3px solid var(--accent);
            padding: 10px;
            margin-top: 10px;
            border-radius: 3px;
            font-size: 0.9rem;
        }

        .examples {
            background: var(--bg-alt);
            padding: 10px;
            margin-top: 10px;
            border-radius: 4px;
            font-family: 'Courier New', monospace;
            font-size: 0.85em;
            overflow-x: auto;
        }

        /* Collapsible */
        .collapsible-header {
            cursor: pointer;
            user-select: none;
            display: flex;
            align-items: center;
            padding: 16px;
            transition: background 0.2s;
        }

        .collapsible-header:hover {
            background: var(--bg-alt);
        }

        .collapse-icon {
            margin-right: 8px;
            font-size: 0.9em;
            transition: transform 0.3s;
            display: inline-block;
            font-weight: bold;
        }

        .collapse-icon.collapsed {
            transform: rotate(-90deg);
        }

        .collapsible-content {
            overflow: hidden;
            transition: max-height 0.3s ease-out, opacity 0.3s ease-out;
            padding: 0 16px 16px 16px;
        }

        .collapsible-content.collapsed {
            max-height: 0 !important;
            opacity: 0;
            padding: 0 16px;
        }

        /* Footer */
        .footer {
            text-align: center;
            color: var(--text-muted);
            margin-top: 64px;
            padding-top: 24px;
            border-top: 1px solid var(--border-light);
            font-size: 0.9rem;
        }

        /* Common utility classes */
        .section-container {
            background: white;
            padding: 25px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }

        .section-header {
            margin-top: 16px;
            color: #1f2937;
            font-size: 1.3rem;
        }

        .section-header-first {
            margin-top: 0;
            color: #1f2937;
            font-size: 1.3rem;
        }

        .subsection-header {
            font-size: 1.25rem;
            font-weight: 600;
            margin-top: 2rem;
            margin-bottom: 0.25rem;
            color: #000;
        }

        .subsection-description {
            font-size: 0.9rem;
            color: #666;
            margin-bottom: 1rem;
        }

        .meta-item {
            margin-bottom: 16px;
        }

        .meta-label {
            color: #6b7280;
            font-size: 0.9em;
        }

        .meta-value {
            color: #1f2937;
            font-weight: 500;
            margin-left: 8px;
        }

        .meta-value-mono {
            color: #1f2937;
            font-weight: 500;
            margin-left: 8px;
            font-family: 'Courier New', monospace;
        }

        .divider {
            margin-top: 24px;
            padding-top: 16px;
            border-top: 1px solid #e5e7eb;
        }

        /* Status badges */
        .badge {
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            padding: 2px 8px;
            border-radius: 999px;
        }

        .badge-ok {
            background: #dcfce7;
            color: #166534;
        }

        .badge-error {
            background: #fee2e2;
            color: #b91c1c;
        }

        .badge-warning {
            background: #fef3c7;
            color: #92400e;
        }

        .badge-skipped {
            background: #fef3c7;
            color: #92400e;
        }

        .badge-not-loaded {
            background: #f3f4f6;
            color: #555;
        }

        .badge-na {
            background: #f3f4f6;
            color: #555;
        }

        .badge-no-checks {
            background: #e0e7ff;
            color: #3730a3;
        }

        .badge-not-checked {
            background: #f3f4f6;
            color: #6b7280;
        }

        /* Info boxes */
        .info-box {
            padding: 12px 15px;
            margin-bottom: 15px;
            border-radius: 4px;
            border-left: 4px solid;
        }

        .info-box-success {
            background: #ecfdf5;
            border-left-color: #10b981;
        }

        .info-box-info {
            background: #eff6ff;
            border-left-color: #3b82f6;
        }

        /* Tables */
        .data-table {
            background: #fff;
            border: 1px solid #eee;
            border-radius: 12px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
            overflow: hidden;
            margin-bottom: 30px;
        }

        .data-table table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.95rem;
        }

        .data-table thead {
            background: #fafafa;
            border-bottom: 1px solid #eee;
        }

        .data-table th {
            padding: 12px 16px;
            text-align: left;
            font-size: 0.8rem;
            font-weight: 600;
            color: #666;
            text-transform: uppercase;
        }

        .data-table tbody tr {
            border-bottom: 1px solid #f2f2f2;
        }

        .data-table tbody tr:last-child {
            border-bottom: none;
        }

        .data-table td {
            padding: 12px 16px;
            color: #222;
            text-align: left;
        }

        .data-table td:first-child {
            font-weight: 600;
            color: #64748B;
        }

        .td-bold {
            font-weight: bold;
        }

        .td-error {
            color: #dc2626;
            font-weight: bold;
        }

        /* Insight sections */
        .insight-section {
            margin-bottom: 15px;
        }

        .insight-header {
            margin: 10px 0 8px 0;
            color: #6b7280;
            font-size: 0.95em;
        }

        .insight-content {
            background: white;
            padding: 10px;
            border-radius: 6px;
            border: 1px solid #e5e7eb;
        }

        .stat-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            padding: 15px;
            background: #fafafa;
            border-radius: 6px;
        }

        .stat-item {
            display: flex;
            flex-direction: column;
        }

        .stat-label {
            font-size: 0.85em;
            color: #6b7280;
            margin-bottom: 4px;
        }

        .stat-value {
            font-size: 1.1em;
            font-weight: 600;
            color: #1f2937;
        }

        /* Layout utility classes */
        .mb-10 { margin-bottom: 10px; }
        .mb-15 { margin-bottom: 15px; }
        .mb-20 { margin-bottom: 20px; }
        .mt-30 { margin-top: 30px; }
        .mt-40 { margin-top: 40px; }

        .p-10 { padding: 10px; }
        .p-12 { padding: 12px; }
        .p-15 { padding: 15px; }
        .pt-8 { padding-top: 8px; }
        .pb-24 { padding-bottom: 24px; }

        .flex { display: flex; }
        .flex-col { flex-direction: column; }
        .flex-wrap { flex-wrap: wrap; }
        .flex-center { align-items: center; }
        .flex-between { justify-content: space-between; }
        .flex-gap-8 { gap: 8px; }
        .flex-gap-15 { gap: 15px; }
        .flex-gap-24 { gap: 24px; }
        .flex-1 { flex: 1; }

        .grid-auto-fit { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); }

        /* Typography classes */
        .page-title {
            font-size: 2rem;
            font-weight: 700;
            margin: 0 0 0.5rem 0;
        }

        .section-title {
            font-size: 1.25rem;
            font-weight: 600;
            margin-top: 2rem;
            margin-bottom: 0.25rem;
            color: #000;
        }

        .section-subtitle {
            font-size: 0.9rem;
            color: #666;
            margin-bottom: 1rem;
        }

        .subsection-title {
            color: #1f2937;
            font-size: 1.1rem;
            margin-bottom: 12px;
        }

        .text-muted { color: var(--text-muted); }
        .text-sm { font-size: 0.9rem; }
        .text-bold { font-weight: 600; }

        /* Header metadata */
        .header-meta {
            display: flex;
            gap: 24px;
            flex-wrap: wrap;
            font-size: 0.9rem;
            color: var(--text-muted);
        }

        /* Summary cards section */
        .summary-cards-bordered {
            border-top: 1px solid var(--border-light);
            padding-top: 24px;
            border-bottom: 1px solid var(--border-light);
            padding-bottom: 24px;
        }

        /* Status badge variants for inline use */
        .status-badge-ok {
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            padding: 2px 8px;
            border-radius: 999px;
            background: #dcfce7;
            color: #166534;
        }

        .status-badge-warning {
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            padding: 2px 8px;
            border-radius: 999px;
            background: #fef3c7;
            color: #92400e;
        }

        .status-badge-error {
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            padding: 2px 8px;
            border-radius: 999px;
            background: #fee2e2;
            color: #b91c1c;
        }

        /* Alert boxes */
        .alert-warning {
            background: #fffbeb;
            border-left: 4px solid #f59e0b;
        }

        .alert-success {
            background: #ecfdf5;
            border-left: 4px solid #10b981;
            padding: 15px;
            border-radius: 6px;
            margin-top: 15px;
        }

        .alert-info {
            background: #f0f9ff;
            border: 1px solid #bae6fd;
            padding: 12px 16px;
            border-radius: 6px;
            color: #0c4a6e;
            font-size: 14px;
        }

        .alert-title {
            color: #92400e;
            margin: 0 0 10px 0;
        }

        .alert-text {
            margin: 0;
            color: #78350f;
        }

        .alert-success-title {
            color: #065f46;
            font-weight: 500;
        }

        .alert-success-text {
            color: #047857;
            font-size: 0.9em;
            margin-top: 5px;
        }

        /* Stat pills/badges for insights */
        .stat-pill {
            display: inline-block;
            background: #f3f4f6;
            color: #4b5563;
            padding: 4px 12px;
            border-radius: 16px;
            font-size: 0.85em;
            margin-right: 6px;
        }

        .stat-pill-label {
            color: #9ca3af;
            text-transform: uppercase;
            font-size: 0.8em;
        }

        .stat-pill-value {
            font-weight: 600;
            color: #667eea;
        }

        /* Check badges */
        .check-badge {
            display: inline-block;
            padding: 6px 12px;
            border-radius: 16px;
            font-size: 0.85em;
            font-weight: 500;
        }

        .check-badge-pass {
            background: #d1fae5;
            color: #065f46;
        }

        .check-badge-fail {
            background: #fee2e2;
            color: #991b1b;
        }

        /* Top values bar */
        .top-value-item {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 8px 12px;
            border-radius: 4px;
            margin-bottom: 6px;
        }

        .top-value-label {
            font-weight: 500;
            color: #1f2937;
        }

        .top-value-pct {
            float: right;
            font-weight: bold;
            color: #1f2937;
            margin-left: 8px;
        }

        .top-value-count {
            float: right;
            color: #6b7280;
        }

        .top-value-stats {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        /* Distribution visualization */
        .distribution-container {
            position: relative;
            height: 100px;
            margin: 10px 0;
            overflow: visible;
        }

        .distribution-gradient {
            position: absolute;
            top: 45px;
            left: 0;
            right: 0;
            height: 8px;
            background: linear-gradient(to right, #e0e7ff 0%, #c7d2fe 25%, #a5b4fc 50%, #818cf8 75%, #6366f1 100%);
            border-radius: 4px;
        }

        .distribution-marker {
            position: absolute;
            top: 40px;
            width: 2px;
            height: 18px;
            background: #4f46e5;
            opacity: 0.7;
        }

        .distribution-marker-bold {
            position: absolute;
            top: 40px;
            width: 3px;
            height: 18px;
            background: #4338ca;
            font-weight: bold;
        }

        .distribution-marker-mean {
            position: absolute;
            top: 43px;
            width: 8px;
            height: 8px;
            background: #f59e0b;
            border: 2px solid white;
            border-radius: 50%;
            transform: translateX(-50%);
        }

        .distribution-label {
            position: absolute;
            font-size: 0.7em;
            font-weight: 600;
            white-space: nowrap;
        }

        .distribution-label-left {
            left: 2%;
        }

        .distribution-label-right {
            right: 2%;
        }

        /* Numeric stats container */
        .numeric-stats-container {
            background: white;
            padding: 8px 10px;
            border-radius: 6px;
            border: 1px solid #e5e7eb;
            display: flex;
            flex-wrap: wrap;
            gap: 15px;
            align-items: center;
        }

        .numeric-stat-item {
            display: inline-flex;
            align-items: baseline;
            gap: 5px;
        }

        .numeric-stat-label {
            color: #9ca3af;
            font-size: 0.75em;
            text-transform: uppercase;
            font-weight: 600;
        }

        .numeric-stat-value {
            font-size: 0.95em;
            font-weight: bold;
            color: #667eea;
        }

        /* Standard deviation footer */
        .std-footer {
            margin-top: 8px;
            padding-top: 8px;
            border-top: 1px solid #f3f4f6;
            font-size: 0.8em;
            color: #6b7280;
            display: flex;
            gap: 15px;
        }

        /* Collapsible section */
        .collapsible-section {
            margin-bottom: 15px;
        }

        /* Checks grid */
        .checks-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
            padding: 15px;
            background: #f9fafb;
            border-radius: 6px;
        }

        .check-metric {
            color: #666;
            font-size: 0.9em;
        }

        .check-value {
            font-size: 1.2em;
            font-weight: bold;
        }

        .check-value-error {
            color: #ef4444;
        }

        .check-value-warning {
            color: #f59e0b;
        }

        .check-value-neutral {
            color: #6b7280;
        }

        .checks-performed-section {
            margin-bottom: 20px;
        }

        .checks-performed-title {
            color: #4b5563;
            margin: 0 0 10px 0;
            font-size: 0.95em;
        }

        .checks-badges-container {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
        }

        /* Relationship section */
        .relationships-section {
            margin-top: 40px;
        }

        .relationships-title {
            font-size: 24px;
            font-weight: 600;
            color: #1f2937;
            margin-bottom: 20px;
        }

        .relationships-description {
            color: #6b7280;
            margin-bottom: 20px;
        }

        .relationships-network {
            width: 100%;
            height: 400px;
            border: 1px solid #e5e7eb;
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }

        .relationships-table-title {
            font-size: 18px;
            font-weight: 600;
            color: #1f2937;
            margin-bottom: 16px;
            margin-top: 30px;
        }

        /* Relationship table */
        .relationship-table {
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }

        .relationship-table thead {
            background-color: #f9fafb;
            border-bottom: 2px solid #e5e7eb;
        }

        .relationship-table th {
            padding: 12px;
            text-align: left;
            font-weight: 600;
            color: #4b5563;
            font-size: 13px;
        }

        .relationship-table th.center {
            text-align: center;
        }

        .relationship-table th.col-arrow {
            width: 40px;
        }

        .relationship-table th.col-confidence {
            width: 200px;
        }

        .relationship-table th.col-type {
            width: 120px;
        }

        .relationship-table th.col-matching {
            width: 100px;
        }

        .relationship-table tbody tr {
            border-bottom: 1px solid #f3f4f6;
        }

        .relationship-table td {
            padding: 12px;
        }

        .relationship-table td.center {
            text-align: center;
        }

        .relationship-table .arrow {
            color: #6606dc;
            font-size: 18px;
        }

        .relationship-cell-table {
            font-weight: 500;
            color: #1f2937;
        }

        .relationship-cell-column {
            font-size: 12px;
            color: #6b7280;
            font-family: 'Courier New', monospace;
        }

        .confidence-bar-container {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .confidence-bar-bg {
            flex: 1;
            height: 8px;
            background: #f3f4f6;
            border-radius: 4px;
            overflow: hidden;
        }

        .confidence-bar-fill {
            height: 100%;
        }

        .confidence-pct {
            font-size: 13px;
            font-weight: 600;
            min-width: 42px;
        }

        .relationship-type-badge {
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: 500;
        }

        .relationship-matching {
            font-weight: 500;
            color: #4b5563;
        }

"""


def _generate_javascript() -> str:
    """Generate JavaScript for interactive features"""
    return """
        function toggleCollapse(id) {
            const content = document.getElementById(id);
            const icon = document.getElementById(id + '-icon');

            if (content.classList.contains('collapsed')) {
                content.classList.remove('collapsed');
                content.style.maxHeight = content.scrollHeight + 'px';
                icon.classList.remove('collapsed');
            } else {
                content.classList.add('collapsed');
                content.style.maxHeight = '0';
                icon.classList.add('collapsed');
            }
        }

        // Initialize all collapsible sections as collapsed on page load
        window.addEventListener('DOMContentLoaded', function() {
            document.querySelectorAll('.collapsible-content').forEach(function(element) {
                // Start collapsed by default
                element.classList.add('collapsed');
                element.style.maxHeight = '0';
            });

            document.querySelectorAll('.collapse-icon').forEach(function(icon) {
                // Set icons to collapsed state
                icon.classList.add('collapsed');
            });
        });

        // Tab switching logic using data-tab attributes
        window.addEventListener('DOMContentLoaded', function() {
            const tabs = document.querySelectorAll('.tab');
            const contents = document.querySelectorAll('.tab-content');

            tabs.forEach(tab => {
                tab.addEventListener('click', () => {
                    // Remove active class from all tabs
                    tabs.forEach(t => t.classList.remove('active'));
                    tab.classList.add('active');

                    // Remove active class from all content sections
                    contents.forEach(c => c.classList.remove('active'));
                    // Show the selected tab content using data-tab attribute
                    document.getElementById(tab.dataset.tab).classList.add('active');
                });
            });
        });

"""
