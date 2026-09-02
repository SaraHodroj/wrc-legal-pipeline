"""Model behaviour that the rest of the pipeline silently depends on."""

from wrc_pipeline.models import DocumentType


def test_content_type_header_beats_url_extension():
    """The site serves documents from handler URLs; the header is authoritative."""
    result = DocumentType.from_url_or_content_type(
        "https://example.ie/getfile.ashx?id=1.html", "application/pdf"
    )
    assert result is DocumentType.PDF


def test_url_extension_is_the_fallback():
    result = DocumentType.from_url_or_content_type("https://example.ie/doc.docx", None)
    assert result is DocumentType.DOCX


def test_query_string_does_not_confuse_extension_detection():
    result = DocumentType.from_url_or_content_type("https://example.ie/a.pdf?v=2", None)
    assert result is DocumentType.PDF


def test_docx_content_type_is_not_mistaken_for_doc():
    """'wordprocessingml' contains no 'msword', but ordering bugs are easy here."""
    ct = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    assert DocumentType.from_url_or_content_type("https://x/y", ct) is DocumentType.DOCX


def test_unknown_when_nothing_matches():
    result = DocumentType.from_url_or_content_type("https://example.ie/file", None)
    assert result is DocumentType.UNKNOWN
