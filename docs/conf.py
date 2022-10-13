project = 'PostgreSQL for novices'
copyright = '2022, Heikkilä et al'
author = 'Heikkilä et al.'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.todo',
    'sphinx_autodoc_typehints',
    'myst_parser',
]

source_suffix = {
    '.rst': 'restructuredtext',
    '.txt': 'markdown',
    '.md': 'markdown',
}

templates_path = ['_templates']
exclude_patterns = ['build', 'Thumbs.db', '.DS_Store']

language = 'en'

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

todo_include_todos = True

always_document_param_types = False
typehints_document_rtype = True
typehints_use_rtype = False
typehints_fully_qualified = False

myst_heading_anchors = 2

html_theme_options = {
    'navigation_depth': 3,
    'collapse_navigation': False,
    'style_external_links': True,
}
