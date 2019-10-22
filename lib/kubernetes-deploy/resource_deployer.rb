# frozen_string_literal: true

require 'kubernetes-deploy/resource_watcher'

module KubernetesDeploy
  class ResourceDeployer
    extend KubernetesDeploy::StatsD::MeasureMethods

    delegate :logger, to: :@task_config

    def initialize(task_config, prune_whitelist, max_watch_seconds, current_sha = nil, selector, global_mode)
      @task_config = task_config
      @prune_whitelist = prune_whitelist
      @max_watch_seconds = max_watch_seconds
      @current_sha = current_sha
      @selector = selector
      @global_mode = global_mode
    end

    def deploy_all_resources(resources, prune: false, verify:, record_summary: true)
      deploy_resources(resources, prune: prune, verify: verify, record_summary: record_summary)
    end
    measure_method(:deploy_all_resources, 'normal_resources.duration')

    private

    def deploy_resources(resources, prune: false, verify:, record_summary: true)
      return if resources.empty?
      deploy_started_at = Time.now.utc

      if resources.length > 1
        logger.info("Deploying resources:")
        resources.each do |r|
          logger.info("- #{r.id} (#{r.pretty_timeout_type})")
        end
      else
        resource = resources.first
        logger.info("Deploying #{resource.id} (#{resource.pretty_timeout_type})")
      end

      # Apply can be done in one large batch, the rest have to be done individually
      applyables, individuals = resources.partition { |r| r.deploy_method == :apply }
      # Prunable resources should also applied so that they can  be pruned
      pruneable_types = @prune_whitelist.map { |t| t.split("/").last }
      applyables += individuals.select { |r| pruneable_types.include?(r.type) }

      individuals.each do |r|
        r.deploy_started_at = Time.now.utc
        case r.deploy_method
        when :replace
          _, _, replace_st = kubectl.run("replace", "-f", r.file_path, log_failure: false)
        when :replace_force
          _, _, replace_st = kubectl.run("replace", "--force", "--cascade", "-f", r.file_path,
            log_failure: false)
        else
          # Fail Fast! This is a programmer mistake.
          raise ArgumentError, "Unexpected deploy method! (#{r.deploy_method.inspect})"
        end

        next if replace_st.success?
        # it doesn't exist so we can't replace it
        _, err, create_st = kubectl.run("create", "-f", r.file_path, log_failure: false)

        next if create_st.success?
        raise FatalDeploymentError, <<~MSG
          Failed to replace or create resource: #{r.id}
          #{err}
        MSG
      end

      apply_all(applyables, prune)

      if verify
        watcher = KubernetesDeploy::ResourceWatcher.new(resources: resources, deploy_started_at: deploy_started_at,
          timeout: @max_watch_seconds, task_config: @task_config, sha: @current_sha)
        watcher.run(record_summary: record_summary)
      end
    end

    def apply_all(resources, prune)
      return unless resources.present?
      command = %w(apply)

      Dir.mktmpdir do |tmp_dir|
        resources.each do |r|
          FileUtils.symlink(r.file_path, tmp_dir)
          r.deploy_started_at = Time.now.utc
        end
        command.push("-f", tmp_dir)

        if prune
          command.push("--prune")
          if @selector
            command.push("--selector", @selector.to_s)
          else
            command.push("--all")
          end
          @prune_whitelist.each { |type| command.push("--prune-whitelist=#{type}") }
        end

        output_is_sensitive = resources.any?(&:sensitive_template_content?)
        out, err, st = kubectl.run(*command, log_failure: false, output_is_sensitive: output_is_sensitive,
          use_namespace: !@global_mode)

        if st.success?
          log_pruning(out) if prune
        else
          record_apply_failure(err, resources: resources)
          raise FatalDeploymentError, "Command failed: #{Shellwords.join(command)}"
        end
      end
    end
    measure_method(:apply_all)

    def log_pruning(kubectl_output)
      pruned = kubectl_output.scan(/^(.*) pruned$/)
      return unless pruned.present?

      logger.info("The following resources were pruned: #{pruned.join(', ')}")
      logger.summary.add_action("pruned #{pruned.length} #{'resource'.pluralize(pruned.length)}")
    end

    def record_apply_failure(err, resources: [])
      warn_msg = "WARNING: Any resources not mentioned in the error(s) below were likely created/updated. " \
        "You may wish to roll back this deploy."
      logger.summary.add_paragraph(ColorizedString.new(warn_msg).yellow)

      unidentified_errors = []
      filenames_with_sensitive_content = resources
        .select(&:sensitive_template_content?)
        .map { |r| File.basename(r.file_path) }

      server_dry_run_validated_resource = resources
        .select(&:server_dry_run_validated?)
        .map { |r| File.basename(r.file_path) }

      err.each_line do |line|
        bad_files = find_bad_files_from_kubectl_output(line)
        unless bad_files.present?
          unidentified_errors << line
          next
        end

        bad_files.each do |f|
          err_msg = f[:err]
          if filenames_with_sensitive_content.include?(f[:filename])
            # Hide the error and template contents in case it has sensitive information
            # we display full error messages as we assume there's no sensitive info leak after server-dry-run
            err_msg = "SUPPRESSED FOR SECURITY" unless server_dry_run_validated_resource.include?(f[:filename])
            record_invalid_template(err: err_msg, filename: f[:filename], content: nil)
          else
            record_invalid_template(err: err_msg, filename: f[:filename], content: f[:content])
          end
        end
      end
      return unless unidentified_errors.any?

      if (filenames_with_sensitive_content - server_dry_run_validated_resource).present?
        warn_msg = "WARNING: There was an error applying some or all resources. The raw output may be sensitive and " \
          "so cannot be displayed."
        logger.summary.add_paragraph(ColorizedString.new(warn_msg).yellow)
      else
        heading = ColorizedString.new('Unidentified error(s):').red
        msg = FormattedLogger.indent_four(unidentified_errors.join)
        logger.summary.add_paragraph("#{heading}\n#{msg}")
      end
    end

    # Inspect the file referenced in the kubectl stderr
    # to make it easier for developer to understand what's going on
    def find_bad_files_from_kubectl_output(line)
      # stderr often contains one or more lines like the following, from which we can extract the file path(s):
      # Error from server (TypeOfError): error when creating "/path/to/service-gqq5oh.yml": Service "web" is invalid:

      line.scan(%r{"(/\S+\.ya?ml\S*)"}).each_with_object([]) do |matches, bad_files|
        matches.each do |path|
          content = File.read(path) if File.file?(path)
          bad_files << { filename: File.basename(path), err: line, content: content }
        end
      end
    end

    def record_invalid_template(err:, filename:, content: nil)
      debug_msg = ColorizedString.new("Invalid template: #{filename}\n").red
      debug_msg += "> Error message:\n#{FormattedLogger.indent_four(err)}"
      if content
        debug_msg += if content =~ /kind:\s*Secret/
          "\n> Template content: Suppressed because it may contain a Secret"
        else
          "\n> Template content:\n#{FormattedLogger.indent_four(content)}"
        end
      end
      logger.summary.add_paragraph(debug_msg)
    end

    def kubectl
      @kubectl ||= Kubectl.new(task_config: @task_config, log_failure_by_default: true)
    end
  end
end
