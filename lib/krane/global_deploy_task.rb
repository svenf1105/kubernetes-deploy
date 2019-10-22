# frozen_string_literal: true
require 'tempfile'

require 'kubernetes-deploy/common'
require 'kubernetes-deploy/concurrency'
require 'kubernetes-deploy/resource_cache'
require 'kubernetes-deploy/kubectl'
require 'kubernetes-deploy/kubeclient_builder'
require 'kubernetes-deploy/cluster_resource_discovery'
require 'kubernetes-deploy/template_sets'
require 'kubernetes-deploy/resource_deployer'

require 'kubernetes-deploy/kubernetes_resource'
%w(
  custom_resource
  custom_resource_definition
).each do |subresource|
  require "kubernetes-deploy/kubernetes_resource/#{subresource}"
end

require 'krane/global_deploy_task_config_validator'

module Krane
  # Ship global resources to a context
  class GlobalDeployTask
    extend KubernetesDeploy::StatsD::MeasureMethods
    delegate :context, :logger, to: :@task_config

    # Initializes the deploy task
    #
    # @param namespace [String] Kubernetes namespace
    # @param context [String] Kubernetes context
    # @param logger [Object] Logger object (defaults to an instance of KubernetesDeploy::FormattedLogger)
    # @param kubectl_instance [Kubectl] Kubectl instance
    # @param bindings [Hash] Bindings parsed by KubernetesDeploy::BindingsParser
    # @param max_watch_seconds [Integer] Timeout in seconds
    # @param selector [Hash] Selector(s) parsed by KubernetesDeploy::LabelSelector
    # @param template_paths [Array<String>] An array of template paths
    # @param template_dir [String] Path to a directory with templates (deprecated)
    # @param protected_namespaces [Array<String>] Array of protected Kubernetes namespaces (defaults
    #   to KubernetesDeploy::DeployTask::PROTECTED_NAMESPACES)
    # @param render_erb [Boolean] Enable ERB rendering
    def initialize(context:, max_watch_seconds: nil, selector: nil, template_paths: [])
      template_paths = template_paths.map { |path| File.expand_path(path) }

      @task_config = ::KubernetesDeploy::TaskConfig.new(context, nil)
      @template_sets = ::KubernetesDeploy::TemplateSets.from_dirs_and_files(paths: template_paths,
        logger: @task_config.logger)
      @max_watch_seconds = max_watch_seconds
      @selector = selector
    end

    # Runs the task, returning a boolean representing success or failure
    #
    # @return [Boolean]
    def run(*args)
      run!(*args)
      true
    rescue KubernetesDeploy::FatalDeploymentError
      false
    end

    # Runs the task, raising exceptions in case of issues
    #
    # @param verify_result [Boolean] Wait for completion and verify success
    # @param allow_protected_ns [Boolean] Enable deploying to protected namespaces
    # @param prune [Boolean] Enable deletion of resources that do not appear in the template dir
    #
    # @return [nil]
    def run!(verify_result: true, prune: true)
      start = Time.now.utc
      logger.reset

      logger.phase_heading("Initializing deploy")
      validate_configuration
      resources = discover_resources
      validate_resources(resources)

      logger.phase_heading("Checking initial resource statuses")
      check_initial_status(resources)

      logger.phase_heading("Deploying all resources")
      deploy!(resources, verify_result, prune, start)
    end

    private

    def deploy!(resources, verify_result, prune, start)
      prune_whitelist = []
      deployer = KubernetesDeploy::ResourceDeployer.new(@task_config,
        prune_whitelist, @max_watch_seconds, @selector, true, statsd_tags)
      if verify_result
        deployer.deploy_all_resources(resources, prune: prune, verify: true)
        failed_resources = resources.reject(&:deploy_succeeded?)
        success = failed_resources.empty?
        if !success && failed_resources.all?(&:deploy_timed_out?)
          raise KubernetesDeploy::DeploymentTimeoutError
        end
        raise KubernetesDeploy::FatalDeploymentError unless success
      else
        deployer.deploy_all_resources(resources, prune: prune, verify: false)
        logger.summary.add_action("deployed #{resources.length} #{'resource'.pluralize(resources.length)}")
        warning = <<~MSG
          Deploy result verification is disabled for this deploy.
          This means the desired changes were communicated to Kubernetes, but the deploy did not make sure they actually succeeded.
        MSG
        logger.summary.add_paragraph(ColorizedString.new(warning).yellow)
      end

      StatsD.event("Deployment succeeded",
        "Successfully deployed all resources to #{context}",
        alert_type: "success", tags: statsd_tags << "status:success")
      StatsD.distribution('all_resources.duration', KubernetesDeploy::StatsD.duration(start), tags: statsd_tags << "status:success")
      logger.print_summary(:success)

    rescue KubernetesDeploy::DeploymentTimeoutError
      logger.print_summary(:timed_out)
      StatsD.event("Deployment timed out",
        "One or more resources failed to deploy to #{context} in time",
        alert_type: "error", tags: statsd_tags << "status:timeout")
      StatsD.distribution('all_resources.duration', KubernetesDeploy::StatsD.duration(start), tags: statsd_tags << "status:timeout")
      raise
    rescue KubernetesDeploy::FatalDeploymentError => error
      logger.summary.add_action(error.message) if error.message != error.class.to_s
      logger.print_summary(:failure)
      StatsD.event("Deployment failed",
        "One or more resources failed to deploy to #{context}",
        alert_type: "error", tags: statsd_tags << "status:failed")
      StatsD.distribution('all_resources.duration', KubernetesDeploy::StatsD.duration(start), tags: statsd_tags << "status:failed")
      raise
    end

    def validate_configuration
      task_config_validator = Krane::GlobalDeployTaskConfigValidator.new(@task_config,
        kubectl, kubeclient_builder)
      errors = []
      errors += task_config_validator.errors
      errors += @template_sets.validate
      unless errors.empty?
        logger.summary.add_action("Configuration invalid")
        logger.summary.add_paragraph(errors.map { |err| "- #{err}" }.join("\n"))
        raise KubernetesDeploy::TaskConfigurationError
      end

      logger.info("Using resource selector #{@selector}") if @selector
      logger.info("All required parameters and files are present")
    end
    measure_method(:validate_configuration)

    def discover_resources
      logger.info("Discovering resources:")
      resources = []
      crds_by_kind = cluster_resource_discoverer.crds.group_by(&:kind)
      @template_sets.with_resource_definitions do |r_def|
        crd = crds_by_kind[r_def["kind"]]&.first
        r = KubernetesDeploy::KubernetesResource.build(context: context, logger: logger, definition: r_def,
          crd: crd, global_names: global_resource_kinds, statsd_tags: statsd_tags)
        resources << r
        logger.info("  - #{r.id}")
      end

      resources.sort
    rescue KubernetesDeploy::InvalidTemplateError => e
      record_invalid_template(err: e.message, filename: e.filename, content: e.content)
      raise KubernetesDeploy::FatalDeploymentError, "Failed to render and parse template"
    end
    measure_method(:discover_resources)

    def cluster_resource_discoverer
      @cluster_resource_discoverer ||= KubernetesDeploy::ClusterResourceDiscovery.new(task_config: @task_config)
    end

    def validate_resources(resources)
      validate_globals(resources)

      KubernetesDeploy::Concurrency.split_across_threads(resources) do |r|
        r.validate_definition(kubectl, selector: @selector)
      end

      resources.select(&:has_warnings?).each do |resource|
        record_warnings(warning: resource.validation_warning_msg, filename: File.basename(resource.file_path))
      end

      failed_resources = resources.select(&:validation_failed?)
      if failed_resources.present?
        failed_resources.each do |r|
          content = File.read(r.file_path) if File.file?(r.file_path) && !r.sensitive_template_content?
          record_invalid_template(err: r.validation_error_msg, filename: File.basename(r.file_path), content: content)
        end
        raise KubernetesDeploy::FatalDeploymentError, "Template validation failed"
      end
    end
    measure_method(:validate_resources)

    def validate_globals(resources)
      return unless (namespaced = resources.reject(&:global?).presence)
      namespaced_names = namespaced.map do |resource|
        "#{resource.name} (#{resource.type}) in #{File.basename(resource.file_path)}"
      end
      namespaced_names = KubernetesDeploy::FormattedLogger.indent_four(namespaced_names.join("\n"))

      logger.summary.add_paragraph(ColorizedString.new("Namespaced resources:\n#{namespaced_names}").yellow)
      raise KubernetesDeploy::FatalDeploymentError, "Deploying namespaced resource is not allowed from this command."
    end

    def statsd_tags
      %W(context:#{@context})
    end

    ### Could be common
    def kubectl
      @kubectl ||= KubernetesDeploy::Kubectl.new(task_config: @task_config, log_failure_by_default: true)
    end

    def kubeclient_builder
      @kubeclient_builder ||= KubernetesDeploy::KubeclientBuilder.new
    end

    def global_resource_kinds
      cluster_resource_discoverer.global_resource_kinds
    end

    def record_invalid_template(err:, filename:, content: nil)
      debug_msg = ColorizedString.new("Invalid template: #{filename}\n").red
      debug_msg += "> Error message:\n#{KubernetesDeploy::FormattedLogger.indent_four(err)}"
      if content
        debug_msg += if content =~ /kind:\s*Secret/
          "\n> Template content: Suppressed because it may contain a Secret"
        else
          "\n> Template content:\n#{KubernetesDeploy::FormattedLogger.indent_four(content)}"
        end
      end
      logger.summary.add_paragraph(debug_msg)
    end

    def record_warnings(warning:, filename:)
      warn_msg = "Template warning: #{filename}\n"
      warn_msg += "> Warning message:\n#{KubernetesDeploy::FormattedLogger.indent_four(warning)}"
      logger.summary.add_paragraph(ColorizedString.new(warn_msg).yellow)
    end

    def check_initial_status(resources)
      @task_config.global_kinds = global_resource_kinds.map(&:downcase)
      cache = KubernetesDeploy::ResourceCache.new(@task_config)
      KubernetesDeploy::Concurrency.split_across_threads(resources) { |r| r.sync(cache) }
      resources.each { |r| logger.info(r.pretty_status) }
    end
    measure_method(:check_initial_status, "initial_status.duration")
  end
end
