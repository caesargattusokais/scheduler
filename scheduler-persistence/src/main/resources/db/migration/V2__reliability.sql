ALTER TABLE execution ADD COLUMN cancel_requested BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE execution ADD COLUMN dead_letter      BOOLEAN NOT NULL DEFAULT false;