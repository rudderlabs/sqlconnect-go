package odpsclient

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParityWithDestination fails when a declaration copied from the
// rudder-server MaxCompute destination differs from the destination's copy in
// testdata/destination. The copy is the shared code of HLD OQ6 option 1: the
// endpoint check, the account constructors, the credential guard, the
// redaction patterns, SafeError and the ODPS code table.
func TestParityWithDestination(t *testing.T) {
	for _, pair := range []struct{ ours, theirs string }{
		{"odps.go", "testdata/destination/odps.go.txt"},
		{"odps_credentials.go", "testdata/destination/odps_credentials.go.txt"},
	} {
		t.Run(pair.ours, func(t *testing.T) {
			ours := declarations(t, pair.ours)
			theirs := declarations(t, pair.theirs)
			require.NotEmpty(t, ours)
			for name, src := range ours {
				want, ok := theirs[name]
				require.Truef(t, ok, "%s declares %s, which the destination does not: add new code to another file", pair.ours, name)
				require.Equalf(t, want, src, "%s drifted from the destination", name)
			}
		})
	}
}

// TestParityCoversThePatterns pins the redaction patterns by name, so a pattern
// moved out of odps.go is noticed rather than silently dropped from the parity check.
func TestParityCoversThePatterns(t *testing.T) {
	ours := declarations(t, "odps.go")
	for _, name := range []string{
		"odpsProjectName", "odpsRegionName", "odpsErrorCodePattern", "odpsNamedCodePattern", "odpsURLPattern",
		"odpsCredentialPattern", "odpsBearerPattern", "odpsOpaqueKeyPattern", "odpsRAMPrincipal", "odpsRAMArn",
		"ValidateEndpoint", "NewWithEndpoint", "NewWithAccessKey", "SafeError", "odpsKnownCode", "odpsDiagnostic",
	} {
		require.Contains(t, ours, name)
	}
	creds := declarations(t, "odps_credentials.go")
	for _, name := range []string{"guardCredentials", "odpsCredentialModel", "odpsCredentialGuard.GetCredential"} {
		require.Contains(t, creds, name)
	}
}

// declarations maps each top-level function, method, type and package-level
// variable in path to its printed source.
func declarations(t *testing.T, path string) map[string]string {
	t.Helper()
	src, err := os.ReadFile(path)
	require.NoError(t, err)
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.SkipObjectResolution)
	require.NoError(t, err)
	print := func(node any) string {
		var b bytes.Buffer
		require.NoError(t, printer.Fprint(&b, fset, node))
		return b.String()
	}
	out := map[string]string{}
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			name := d.Name.Name
			if d.Recv != nil && len(d.Recv.List) == 1 {
				recv := d.Recv.List[0].Type
				if star, ok := recv.(*ast.StarExpr); ok {
					recv = star.X
				}
				if ident, ok := recv.(*ast.Ident); ok {
					name = ident.Name + "." + name
				}
			}
			d.Doc = nil
			out[name] = print(d)
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.ValueSpec:
					s.Doc, s.Comment = nil, nil
					for i, ident := range s.Names {
						if i < len(s.Values) {
							out[ident.Name] = print(s.Values[i])
						} else {
							out[ident.Name] = print(s.Type)
						}
					}
				case *ast.TypeSpec:
					s.Doc, s.Comment = nil, nil
					out[s.Name.Name] = print(s)
				}
			}
		}
	}
	return out
}
