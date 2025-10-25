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
        }

        .meta-label {
            font-size: 0.75rem;
            text-transform: uppercase;
            color: var(--text-muted);
            letter-spacing: 0.5px;
        }

        .meta-value {
            font-size: 0.95rem;
            font-weight: 500;
            color: var(--accent);
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
