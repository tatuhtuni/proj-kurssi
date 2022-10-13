project = 'PostgreSQL for novices'
copyright = '2022, Heikkilä et al.'
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

todo_include_todos = True

always_document_param_types  = False
typehints_document_rtype = True
typehints_use_rtype = False
typehints_fully_qualified = False
