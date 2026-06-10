package server

import (
	"fmt"
	"qvdb/tsdb"
	"strconv"
	"strings"
	"unicode"

	"github.com/mababaNiubi/variant"
)

type tokenKind int

const (
	tokEOF tokenKind = iota
	tokKeyword
	tokIdent
	tokString
	tokNumber
	tokOperator
	tokComma
	tokLParen
	tokRParen
	tokStar
)

type token struct {
	kind  tokenKind
	value string
}

type lexer struct {
	input []rune
	pos   int
}

func newLexer(input string) *lexer {
	return &lexer{input: []rune(input), pos: 0}
}

func (l *lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(l.input[l.pos]) {
		l.pos++
	}
}

func (l *lexer) peek() rune {
	if l.pos < len(l.input) {
		return l.input[l.pos]
	}
	return 0
}

var keywords = map[string]bool{
	"create": true, "table": true, "type": true, "precision": true,
	"insert": true, "into": true, "values": true,
	"select": true, "from": true, "where": true, "and": true, "or": true,
	"limit": true, "polymerization": true, "having": true,
	"true": true, "false": true,
	"float": true, "int": true, "integer": true, "string": true, "bool": true, "json": true,
	"latest": true,
}

func (l *lexer) nextToken() (token, error) {
	l.skipWhitespace()
	if l.pos >= len(l.input) {
		return token{kind: tokEOF}, nil
	}

	ch := l.peek()

	switch {
	case ch == '\'':
		return l.scanString()
	case ch == ',':
		l.pos++
		return token{kind: tokComma, value: ","}, nil
	case ch == '(':
		l.pos++
		return token{kind: tokLParen, value: "("}, nil
	case ch == ')':
		l.pos++
		return token{kind: tokRParen, value: ")"}, nil
	case ch == '*':
		l.pos++
		return token{kind: tokStar, value: "*"}, nil
	case ch == '>' || ch == '<' || ch == '=' || ch == '!':
		return l.scanOperator()
	case unicode.IsLetter(ch) || ch == '_':
		return l.scanIdent()
	case unicode.IsDigit(ch) || ch == '-':
		return l.scanNumber()
	default:
		return token{}, fmt.Errorf("unexpected character: %c", ch)
	}
}

func (l *lexer) scanString() (token, error) {
	l.pos++ // skip opening quote
	var sb strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		l.pos++
		if ch == '\'' {
			if l.pos < len(l.input) && l.input[l.pos] == '\'' {
				sb.WriteRune('\'')
				l.pos++
			} else {
				return token{kind: tokString, value: sb.String()}, nil
			}
		} else {
			sb.WriteRune(ch)
		}
	}
	return token{}, fmt.Errorf("unterminated string")
}

func (l *lexer) scanOperator() (token, error) {
	ch := l.input[l.pos]
	l.pos++
	if l.pos < len(l.input) && l.input[l.pos] == '=' {
		l.pos++
		return token{kind: tokOperator, value: string([]rune{ch, '='})}, nil
	}
	return token{kind: tokOperator, value: string(ch)}, nil
}

func (l *lexer) scanIdent() (token, error) {
	start := l.pos
	for l.pos < len(l.input) && (unicode.IsLetter(l.input[l.pos]) || unicode.IsDigit(l.input[l.pos]) || l.input[l.pos] == '_') {
		l.pos++
	}
	word := string(l.input[start:l.pos])
	lower := strings.ToLower(word)
	if keywords[lower] {
		return token{kind: tokKeyword, value: lower}, nil
	}
	return token{kind: tokIdent, value: word}, nil
}

func (l *lexer) scanNumber() (token, error) {
	start := l.pos
	if l.input[l.pos] == '-' {
		l.pos++
	}
	hasDot := false
	for l.pos < len(l.input) && (unicode.IsDigit(l.input[l.pos]) || l.input[l.pos] == '.') {
		if l.input[l.pos] == '.' {
			if hasDot {
				break
			}
			hasDot = true
		}
		l.pos++
	}
	return token{kind: tokNumber, value: string(l.input[start:l.pos])}, nil
}

type parser struct {
	lex    *lexer
	tok    token
	err    error
	peeked bool
}

func newParser(input string) *parser {
	return &parser{lex: newLexer(input)}
}

func (p *parser) next() token {
	if p.err != nil {
		return token{kind: tokEOF}
	}
	if p.peeked {
		p.peeked = false
		return p.tok
	}
	tok, err := p.lex.nextToken()
	if err != nil {
		p.err = err
		return token{kind: tokEOF}
	}
	p.tok = tok
	return tok
}

func (p *parser) peek() token {
	if p.peeked {
		return p.tok
	}
	p.tok = p.next()
	p.peeked = true
	return p.tok
}

func (p *parser) expect(kind tokenKind, value string) bool {
	tok := p.next()
	if tok.kind != kind {
		p.err = fmt.Errorf("expected %v, got %v (%q)", kind, tok.kind, tok.value)
		return false
	}
	if value != "" && tok.value != value {
		p.err = fmt.Errorf("expected %q, got %q", value, tok.value)
		return false
	}
	return true
}

func (p *parser) expectKeyword(kw string) bool {
	return p.expect(tokKeyword, kw)
}

func Parse(sql string) (Stmt, error) {
	p := newParser(sql)
	tok := p.next()
	if tok.kind != tokKeyword {
		return nil, fmt.Errorf("expected SQL keyword, got %q", tok.value)
	}
	switch tok.value {
	case "create":
		return p.parseCreateTable()
	case "insert":
		return p.parseInsert()
	case "select":
		if p.peek().kind == tokKeyword && p.peek().value == "latest" {
			p.next() // consume "latest"
			return p.parseSelectLatest()
		}
		return p.parseSelect()
	default:
		return nil, fmt.Errorf("unexpected keyword: %s", tok.value)
	}
}

func (p *parser) parseCreateTable() (Stmt, error) {
	if !p.expectKeyword("table") {
		return nil, p.err
	}
	nameTok := p.next()
	if nameTok.kind != tokIdent {
		return nil, fmt.Errorf("expected table name, got %q", nameTok.value)
	}
	if !p.expectKeyword("type") {
		return nil, p.err
	}
	typeTok := p.next()
	if typeTok.kind != tokKeyword {
		return nil, fmt.Errorf("expected column type, got %q", typeTok.value)
	}
	var colType tsdb.ColumnType
	switch typeTok.value {
	case "float":
		colType = tsdb.ColumnTypeFloat
	case "int", "integer":
		colType = tsdb.ColumnTypeInt
	case "string":
		colType = tsdb.ColumnTypeString
	case "bool":
		colType = tsdb.ColumnTypeBool
	case "json":
		colType = tsdb.ColumnTypeJson
	default:
		return nil, fmt.Errorf("unknown column type: %s", typeTok.value)
	}

	stmt := &CreateTableStmt{Name: nameTok.value, Type: colType}

	if p.peek().kind == tokKeyword && p.peek().value == "precision" {
		p.next()
		precTok := p.next()
		if precTok.kind != tokNumber {
			return nil, fmt.Errorf("expected precision number, got %q", precTok.value)
		}
		prec, err := strconv.ParseUint(precTok.value, 10, 8)
		if err != nil {
			return nil, fmt.Errorf("invalid precision: %s", precTok.value)
		}
		stmt.Precision = uint8(prec)
	}

	return stmt, nil
}

func (p *parser) parseInsert() (Stmt, error) {
	if !p.expectKeyword("into") {
		return nil, p.err
	}
	tableTok := p.next()
	if tableTok.kind != tokIdent {
		return nil, fmt.Errorf("expected table name, got %q", tableTok.value)
	}
	p.expect(tokLParen, "")
	p.expect(tokIdent, "") // tag column name (ignored)
	p.expect(tokComma, "")
	p.expect(tokIdent, "") // time column name (ignored)
	p.expect(tokComma, "")
	p.expect(tokIdent, "") // value column name (ignored)
	p.expect(tokRParen, "")
	if !p.expectKeyword("values") {
		return nil, p.err
	}

	var rows []InsertRow
	for {
		p.expect(tokLParen, "")

		tagTok := p.next()
		if tagTok.kind != tokString {
			return nil, fmt.Errorf("expected tag string, got %q", tagTok.value)
		}
		p.expect(tokComma, "")

		timeTok := p.next()
		if timeTok.kind != tokNumber {
			return nil, fmt.Errorf("expected timestamp number, got %q", timeTok.value)
		}
		ts, err := strconv.ParseInt(timeTok.value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp: %s", timeTok.value)
		}
		p.expect(tokComma, "")

		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		p.expect(tokRParen, "")

		if p.err != nil {
			return nil, p.err
		}

		rows = append(rows, InsertRow{
			Tag:       tagTok.value,
			Timestamp: ts,
			Value:     val,
		})

		if p.peek().kind == tokComma {
			p.next() // consume comma
		} else {
			break
		}
	}

	return &InsertStmt{
		Table: tableTok.value,
		Rows:  rows,
	}, nil
}

func (p *parser) parseValue() (variant.Variant, error) {
	tok := p.next()
	switch tok.kind {
	case tokString:
		return variant.NewString(tok.value), nil
	case tokNumber:
		if strings.Contains(tok.value, ".") {
			f, err := strconv.ParseFloat(tok.value, 64)
			if err != nil {
				return variant.Variant{}, err
			}
			return variant.NewFloat64(f), nil
		}
		i, err := strconv.ParseInt(tok.value, 10, 64)
		if err != nil {
			return variant.Variant{}, err
		}
		return variant.NewInt64(i), nil
	case tokKeyword:
		switch tok.value {
		case "true":
			return variant.NewBool(true), nil
		case "false":
			return variant.NewBool(false), nil
		}
	}
	return variant.Variant{}, fmt.Errorf("expected value, got %q", tok.value)
}

func (p *parser) parseSelect() (Stmt, error) {
	if !p.expect(tokStar, "*") {
		return nil, p.err
	}
	if !p.expectKeyword("from") {
		return nil, p.err
	}
	tableTok := p.next()
	if tableTok.kind != tokIdent {
		return nil, fmt.Errorf("expected table name, got %q", tableTok.value)
	}

	stmt := &SelectStmt{Table: tableTok.value}

	if p.peek().kind == tokKeyword && p.peek().value == "where" {
		p.next()
		if err := p.parseWhereClause(stmt); err != nil {
			return nil, err
		}
	}

	for p.peek().kind == tokKeyword {
		switch p.peek().value {
		case "limit":
			p.next()
			limitTok := p.next()
			if limitTok.kind != tokNumber {
				return nil, fmt.Errorf("expected limit number, got %q", limitTok.value)
			}
			n, err := strconv.ParseInt(limitTok.value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid limit: %s", limitTok.value)
			}
			stmt.Limit = n
		case "polymerization":
			p.next()
			polyTok := p.next()
			if polyTok.kind != tokIdent && polyTok.kind != tokKeyword {
				return nil, fmt.Errorf("expected polymerization type, got %q", polyTok.value)
			}
			switch strings.ToLower(polyTok.value) {
			case "avg", "min", "max":
				stmt.Polymerization = strings.ToLower(polyTok.value)
			default:
				return nil, fmt.Errorf("unknown polymerization: %s (use avg, min, max)", polyTok.value)
			}
		case "having":
			p.next()
			cond, err := p.parseCondition()
			if err != nil {
				return nil, err
			}
			stmt.Having = cond
		default:
			return nil, fmt.Errorf("unexpected keyword: %s", p.peek().value)
		}
	}

	return stmt, nil
}

func (p *parser) parseWhereClause(stmt *SelectStmt) error {
	for {
		tok := p.next()
		if tok.kind != tokIdent {
			return fmt.Errorf("expected identifier in WHERE, got %q", tok.value)
		}
		ident := tok.value

		opTok := p.next()
		if opTok.kind != tokOperator {
			return fmt.Errorf("expected operator after %s, got %q", ident, opTok.value)
		}

		val, err := p.parseValue()
		if err != nil {
			return err
		}

		lowerIdent := strings.ToLower(ident)
		switch {
		case lowerIdent == "tag" && opTok.value == "=":
			stmt.Tag = val.AsString()
		case lowerIdent == "time" && (opTok.value == ">=" || opTok.value == ">"):
			startTime, err := val.AsInt64()
			if err != nil {
				return fmt.Errorf("invalid start time: %w", err)
			}
			stmt.StartTime = startTime
		case lowerIdent == "time" && (opTok.value == "<=" || opTok.value == "<"):
			stmt.EndTime, _ = val.AsInt64()
		default:
			// Other conditions go to Having
			cond := tsdb.Condition{
				ColumnAttributeName: mapColumnName(ident),
				Type:                tsdb.ConditionOperator(opTok.value),
				Value:               val,
			}
			if stmt.Having != nil {
				if logical, ok := stmt.Having.(tsdb.LogicalCondition); ok && logical.Operator == tsdb.And {
					logical.Conditions = append(logical.Conditions, cond)
					stmt.Having = logical
				} else {
					stmt.Having = tsdb.LogicalCondition{
						Operator:   tsdb.And,
						Conditions: []any{stmt.Having.(tsdb.Condition), cond},
					}
				}
			} else {
				stmt.Having = cond
			}
		}

		if p.peek().kind != tokKeyword || strings.ToLower(p.peek().value) != "and" {
			break
		}
		p.next() // consume AND
	}
	return nil
}

func (p *parser) parseSelectLatest() (Stmt, error) {
	tableTok := p.next()
	if tableTok.kind != tokIdent {
		return nil, fmt.Errorf("expected table name, got %q", tableTok.value)
	}

	tagTok := p.next()
	if tagTok.kind != tokString && tagTok.kind != tokIdent {
		return nil, fmt.Errorf("expected tag, got %q", tagTok.value)
	}

	return &SelectLatestStmt{
		Table: tableTok.value,
		Tag:   tagTok.value,
	}, nil
}

func (p *parser) parseCondition() (tsdb.Condition, error) {
	identTok := p.next()
	if identTok.kind != tokIdent {
		return tsdb.Condition{}, fmt.Errorf("expected identifier in condition, got %q", identTok.value)
	}

	opTok := p.next()
	if opTok.kind != tokOperator {
		return tsdb.Condition{}, fmt.Errorf("expected operator, got %q", opTok.value)
	}

	val, err := p.parseValue()
	if err != nil {
		return tsdb.Condition{}, err
	}

	return tsdb.Condition{
		ColumnAttributeName: mapColumnName(identTok.value),
		Type:                tsdb.ConditionOperator(opTok.value),
		Value:               val,
	}, nil
}

func mapColumnName(name string) string {
	if strings.ToLower(name) == "value" {
		return ""
	}
	return name
}
