<?xml version='1.0'?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version='1.0'
                xmlns="http://www.w3.org/TR/xhtml1/transitional"
                exclude-result-prefixes="#default">

<xsl:import href="http://docbook.sourceforge.net/release/xsl/current/xhtml/docbook.xsl"/>

<xsl:param name="html.stylesheet" select="'stylesheet.css'"></xsl:param>

<xsl:param name="chapter.autolabel" select="0"></xsl:param>
<xsl:param name="xref.with.number.and.title" select="0"></xsl:param>
<xsl:param name="generate.section.toc.level" select="1"></xsl:param>
<xsl:param name="variablelist.term.break.after">1</xsl:param>
<xsl:param name="variablelist.term.separator"></xsl:param>

<xsl:template match="productname">
  <xsl:call-template name="inline.charseq"/>
</xsl:template>

<xsl:param name="generate.toc">
sect1 toc
</xsl:param>

</xsl:stylesheet>
