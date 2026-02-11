import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('ALTER XML schema string literal indentation', () => {
  it('keeps multiline XML string indentation stable across formatting passes', () => {
    const sql = `ALTER XML SCHEMA COLLECTION [Person].[AdditionalContactInfoSchemaCollection] ADD
'<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema targetNamespace="x">
    <xsd:element name="ContactRecord">
    </xsd:element>
</xsd:schema>';`;

    const once = formatSQL(sql);
    const twice = formatSQL(once);

    expect(twice).toBe(once);
  });
});
