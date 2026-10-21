-- name: GetWorkspaceName :one
SELECT name FROM workspaces WHERE id = $1;
