# frozen_string_literal: true

require 'kubernetes-deploy/task_config_validator'

module Krane
  class GlobalDeployTaskConfigValidator < KubernetesDeploy::TaskConfigValidator
    def initialize(*arguments)
      super(*arguments)
      @validations -= [:validate_namespace_exists]
    end
  end
end
