Verbatim copies of rudder-server `warehouse/integrations/maxcompute/client/odps.go` and
`odps_credentials.go` at master `a64bfd9f2`. `parity_test.go` compares every declaration the source
driver copied against them. When the destination changes, refresh both files:

    git -C rudder-server show origin/master:warehouse/integrations/maxcompute/client/odps.go > odps.go.txt
    git -C rudder-server show origin/master:warehouse/integrations/maxcompute/client/odps_credentials.go > odps_credentials.go.txt

then port the change into `../../odps.go` or `../../odps_credentials.go`, or record why the source differs.
