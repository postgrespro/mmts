# Generating documentation
```
xmllint --noout --valid multimaster_book.xml
xsltproc stylesheet.xsl multimaster_book.xml >multimaster.html
```

and don't forget to install the result on postgrespro.github.io:
```
cp multimaster.html stylesheet.css <dir-with-site>/mmts/
```