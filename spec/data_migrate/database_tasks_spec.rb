# frozen_string_literal: true

require "spec_helper"

describe DataMigrate::DatabaseTasks do
  let(:subject) { DataMigrate::DatabaseTasks }
  let(:migration_path) { "spec/db/migrate" }
  let(:data_migrations_path) { DataMigrate.config.data_migrations_path }

  before do
    # In a normal Rails installation, db_dir would defer to
    # Rails.application.config.paths["db"].first
    # @see https://github.com/rails/rails/blob/a7d49ef78c36df2d1ca876451f30915ada1079a5/activerecord/lib/active_record/tasks/database_tasks.rb#L54
    allow(subject).to receive(:db_dir).and_return("db")
    allow(ActiveRecord::Tasks::DatabaseTasks).to receive(:db_dir).and_return("db")
  end

  before do
    allow(DataMigrate::Tasks::DataMigrateTasks).to receive(:migrations_paths) do
      data_migrations_path
    end
    ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: "spec/db/test.db")
    hash_config = ActiveRecord::DatabaseConfigurations::HashConfig.new(
      'test', 'test', adapter: "sqlite3", database: "spec/db/test.db"
    )
    config_obj = ActiveRecord::DatabaseConfigurations.new([hash_config])
    allow(ActiveRecord::Base).to receive(:configurations).and_return(config_obj)
  end

  context "migrations" do
    after do
      ActiveRecord::Migration.drop_table("data_migrations") rescue nil
      ActiveRecord::Migration.drop_table("schema_migrations") rescue nil
    end

    before do
      DataMigrate::RailsHelper.schema_migration.create_table

      allow(DataMigrate::SchemaMigration).to receive(:migrations_paths) { migration_path }
      allow(DataMigrate::DatabaseTasks).to receive(:data_migrations_path) do
        data_migrations_path
      end.at_least(:once)
    end

    describe :past_migrations do
      it "returns past migration records" do
        subject.forward
        migrations = subject.past_migrations
        expect(migrations.count).to eq 1
        expect(migrations.first[:version]).to eq 20091231235959
      end

      it "shows nothing without any migrations" do
        migrations = subject.past_migrations
        expect(migrations.count).to eq 0
      end
    end

    describe :forward do
      it "run forward default amount of times" do
        subject.forward
        versions = DataMigrate::RailsHelper.data_schema_migration.normalized_versions
        expect(versions.count).to eq(1)
      end

      it "run forward defined number of times" do
        subject.forward(2)
        versions = DataMigrate::RailsHelper.data_schema_migration.normalized_versions
        expect(versions.count).to eq(1)
        expect(versions.first).to eq "20091231235959"
        versions = DataMigrate::RailsHelper.schema_migration.normalized_versions
        expect(versions.count).to eq(1)
        expect(versions.first).to eq "20131111111111"
      end
    end

    if DataMigrate::RailsHelper.rails_version_equal_to_or_higher_than_7_0
      describe :schema_dump_path do
        before do
          allow(ActiveRecord::Base).to receive(:configurations)
            .and_return(ActiveRecord::DatabaseConfigurations.new([db_config]))
        end

        context "for primary database" do
          let(:db_config) do
            ActiveRecord::DatabaseConfigurations::HashConfig.new("development", "primary", {})
          end

          context "for :ruby db format" do
            it 'returns the data schema path' do
              allow(ActiveRecord).to receive(:schema_format).and_return(:ruby)
              expect(subject.schema_dump_path(db_config)).to eq("db/data_schema.rb")
            end
          end

          context "for :sql db format" do
            it 'returns the data schema path' do
              allow(ActiveRecord).to receive(:schema_format).and_return(:sql)
              expect(subject.schema_dump_path(db_config, :sql)).to eq("db/data_schema.rb")
            end
          end
        end
      end
    end

    describe :prepare_all_with_data do
      let(:primary_db_config) do
        ActiveRecord::DatabaseConfigurations::HashConfig.new(
          'test',
          'primary',
          adapter: "sqlite3",
          database: "spec/db/test.db"
        ).tap do |db_config|
          db_config.define_singleton_method(:seeds?) { true }
          db_config.define_singleton_method(:schema_format) { :ruby }
        end
      end

      let(:secondary_db_config) do
        ActiveRecord::DatabaseConfigurations::HashConfig.new(
          'test',
          'secondary',
          adapter: "sqlite3",
          database: "spec/db/secondary_test.db"
        ).tap do |db_config|
          db_config.define_singleton_method(:seeds?) { false }
          db_config.define_singleton_method(:schema_format) { :sql }
        end
      end

      let(:primary_pool) { double("PrimaryConnectionPool") }
      let(:secondary_pool) { double("SecondaryConnectionPool") }
      let(:primary_connection) { double("PrimaryConnection") }
      let(:secondary_connection) { double("SecondaryConnection") }
      let(:migration_class) { class_double(ActiveRecord::Base, establish_connection: true) }
      let(:primary_structure_path) { "spec/db/schema.rb" }
      let(:secondary_structure_path) { "spec/db/secondary_schema.rb" }
      let(:primary_data_schema_path) { "spec/db/data_schema.rb" }
      let(:created_configs) { [] }

      before do
        allow(subject).to receive(:each_current_configuration) do |*_args, &block|
          block.call(primary_db_config)
          block.call(secondary_db_config)
        end

        allow(subject).to receive(:with_temporary_pool) do |db_config, &block|
          pool =
            if db_config == primary_db_config
              primary_pool
            else
              secondary_pool
            end

          block.call(pool)
        end

        allow(primary_pool).to receive(:lease_connection).and_return(primary_connection)
        allow(secondary_pool).to receive(:lease_connection).and_return(secondary_connection)
        allow(subject).to receive(:database_exists?).with(primary_connection).and_return(false)
        allow(subject).to receive(:database_exists?).with(secondary_connection).and_return(false)
        allow(subject).to receive(:create) { |db_config| created_configs << db_config }

        # Return a path only for the expected per-database format so this spec
        # fails if prepare_all_with_data resolves schema dumps with the wrong format.
        allow(ActiveRecord::Tasks::DatabaseTasks).to receive(:schema_dump_path) do |db_config, format = nil|
          if db_config == primary_db_config && format == :ruby
            primary_structure_path
          elsif db_config == secondary_db_config && format == :sql
            secondary_structure_path
          end
        end

        allow(File).to receive(:exist?).and_return(false)
        allow(File).to receive(:exist?).with(primary_structure_path).and_return(true)
        allow(File).to receive(:exist?).with(secondary_structure_path).and_return(true)
        allow(File).to receive(:exist?).with(primary_data_schema_path).and_return(true)
        allow(ActiveRecord::Tasks::DatabaseTasks).to receive(:load_schema)
        allow(subject).to receive(:load)
        allow(subject).to receive(:migrate_with_data)
        allow(subject).to receive(:load_seed)
        allow(subject).to receive(:migration_class).and_return(migration_class)
        allow(DataMigrate::Tasks::DataMigrateTasks).to receive(:dump)
        allow(ActiveRecord::Tasks::DatabaseTasks).to receive(:dump_schema)

        configurations = ActiveRecord::DatabaseConfigurations.new([primary_db_config, secondary_db_config])
        allow(ActiveRecord::Base).to receive(:configurations).and_return(configurations)
      end

      it "initializes current databases and only loads data schema for the primary database" do
        subject.prepare_all_with_data

        expect(created_configs).to contain_exactly(primary_db_config, secondary_db_config)
        expect(ActiveRecord::Tasks::DatabaseTasks).to have_received(:load_schema).with(primary_db_config, :ruby, nil)
        expect(ActiveRecord::Tasks::DatabaseTasks).to have_received(:load_schema).with(secondary_db_config, :sql, nil)
        expect(ActiveRecord::Tasks::DatabaseTasks).to have_received(:dump_schema).with(primary_db_config)
        expect(ActiveRecord::Tasks::DatabaseTasks).to have_received(:dump_schema).with(secondary_db_config)
        expect(subject).to have_received(:load).with(primary_data_schema_path).once
        expect(subject).to have_received(:migrate_with_data)
        expect(DataMigrate::Tasks::DataMigrateTasks).to have_received(:dump)
        expect(migration_class).to have_received(:establish_connection).with(subject.env.to_sym).once
        expect(subject).to have_received(:load_seed)
      end

      it "restores the primary connection before seeding when data dump uses an override connection" do
        DataMigrate.config.db_configuration = { adapter: "sqlite3", database: "spec/db/override.db" }

        subject.prepare_all_with_data

        expect(migration_class).to have_received(:establish_connection).with(subject.env.to_sym).twice
      ensure
        DataMigrate.config.db_configuration = nil
      end

      it "skips setup work for existing databases but still migrates and dumps" do
        allow(subject).to receive(:database_exists?).with(primary_connection).and_return(true)
        allow(subject).to receive(:database_exists?).with(secondary_connection).and_return(true)

        subject.prepare_all_with_data

        expect(created_configs).to be_empty
        expect(ActiveRecord::Tasks::DatabaseTasks).not_to have_received(:load_schema)
        expect(subject).not_to have_received(:load)
        expect(subject).to have_received(:migrate_with_data)
        expect(ActiveRecord::Tasks::DatabaseTasks).to have_received(:dump_schema).with(primary_db_config)
        expect(ActiveRecord::Tasks::DatabaseTasks).to have_received(:dump_schema).with(secondary_db_config)
        expect(DataMigrate::Tasks::DataMigrateTasks).to have_received(:dump)
        expect(subject).not_to have_received(:load_seed)
      end
    end
  end
end
